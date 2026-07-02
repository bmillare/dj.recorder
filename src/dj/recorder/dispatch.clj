(ns dj.recorder.dispatch
  "The dispatch core for dj.recorder — alpha item 3.
  A single on-demand virtual thread (the *drainer*) walks a queue of pending
  transactions, applying each in submission order. At most one drainer is alive
  at a time, so transactions form a total order with no per-item locking and a
  race-free read-modify-write (the authoring fn runs serialized, agent-style —
  §9).

  Concurrency design (§2, revised):
    All dispatch state lives in ONE atom holding {:queue PersistentQueue
    :error nil|Throwable}, so \"enqueue\" and \"am I halted?\" are decided in a
    single atomic swap — the halted-vs-enqueue race is unrepresentable (this
    mirrors clojure.lang.Agent's ActionQueue). Drainer liveness is encoded by
    the queue itself: the in-flight item stays at the head until it completes,
    so `queue non-empty` ⇔ `a drainer owns it`. The submitter that performs the
    empty→non-empty transition spawns the drainer; the pop that produces an
    empty queue retires it. No separate draining flag, no recheck window.

  Contracts pinned from the runtime deep-dive:
    §3  PERSIST-THEN-PUBLISH: append the patch durably *first*, then publish to
        readers (the `state` atom / `deref`), then ack the caller. `@db` never
        shows state that isn't on disk.
    §5  Two failure classes via the per-tx promise:
          - authoring/patch error (patch-fn or apply-patch throws): reject just
            this tx (promise -> Throwable), state untouched, drainer continues.
          - I/O append error: HALT — set the core's error, reject all pending
            txs with it, stop the drainer, and refuse further submits (writes
            throw). Reads still work; recover via close! + re-open (item 4).
        An escaped Throwable from the drain loop itself is treated as a HALT
        rather than silently killing the drainer.

  Pure dispatch + the injected `append` fn; no file knowledge of its own
  (storage is item 2, the public API/lifecycle is item 4). `append` is
  `(fn [patch])` that durably persists one patch and may throw on I/O failure."
  (:refer-clojure :exclude [await])
  (:require [dj.recorder.patch :as patch])
  (:import (clojure.lang IDeref PersistentQueue)))

(set! *warn-on-reflection* true)

(def ^:private empty-queue PersistentQueue/EMPTY)

;; ---------------------------------------------------------------------------
;; Protocol + type
;; ---------------------------------------------------------------------------

(defprotocol IRecorderCore
  (submit! [core patch-fn]
    "Enqueue a `state → patch` authoring fn and ensure a drainer is running.
    Returns the per-tx promise: it resolves to the new realized state on
    success, or to a `Throwable` on a patch/authoring or I/O error (§5).
    Submission order is commit order. Throws if the core is halted — writes
    throw on a halted db; reads (`deref`) still work. Recover via the public
    lifecycle's close!/re-open. Enqueue and halted-check are one atomic swap,
    so a submit can never be stranded by a concurrent halt.")
  (await [core]
    "Block until every tx queued *before this call* has been processed
    (agent-style `await`). Implemented as a first-class barrier item: it is
    never authored, applied, or persisted — its promise resolving simply means
    all prior txs drained. Returns nil. On a halted core, returns immediately
    (nothing will drain).")
  (error [core]
    "The I/O Throwable that halted this core, or nil if it is still live (§5).
    Named after `agent-error`."))

(declare enqueue!)

(deftype Core [state     ; atom: last durably-written state (what deref shows)
               dispatch  ; atom: {:queue PersistentQueue, :error nil|Throwable}
               append]   ; (fn [patch]): durable persist; may throw → HALT
  IDeref
  ;; Current realized state — what `@db` reads. Persist-then-publish (§3): the
  ;; last *durably-written* state; may lag the queue. Never blocks on I/O.
  (deref [_] @state)

  IRecorderCore
  (submit! [core patch-fn]
    (let [p (promise)]
      (if-let [err (enqueue! core {:patch-fn patch-fn :promise p})]
        (throw (ex-info "dj.recorder: db is halted by an earlier I/O failure; writes throw, reads still work — close! and re-open to recover"
                        {:dj.recorder/halted true}
                        err))
        p)))

  (await [core]
    (let [p (promise)]
      (when-not (enqueue! core {:barrier? true :promise p})
        @p))
    nil)

  (error [_]
    (:error @dispatch)))

;; ---------------------------------------------------------------------------
;; Per-transaction processing (persist-then-publish; §3 / §5)
;; ---------------------------------------------------------------------------

(defn- process!
  "Run one queued item. Returns `:ok` to keep draining, or the I/O Throwable to
  signal HALT. Barrier items just ack and continue. Authoring/patch errors
  reject only this tx and return `:ok` — a bad patch can neither poison the db
  nor kill the drainer (§5). The authoring fn reads the latest realized state
  here, on the single drainer thread, so read-modify-write is race-free (§9)."
  [^Core core {:keys [patch-fn promise barrier?]}]
  (if barrier?
    (do (deliver promise nil) :ok)
    (let [state (.-state core)
          ;; Phase 1: author + apply, off the current realized state. Any throw
          ;; here is a patch/authoring error: reject this tx, leave state alone.
          computed (try
                     (let [s  @state
                           p  (patch-fn s)
                           s' (patch/apply-patch s p)]
                       {:s s :patch p :s' s'})
                     (catch Throwable e
                       (deliver promise e)
                       nil))]
      (cond
        (nil? computed) :ok                         ; rejected above; continue
        (= (:s' computed) (:s computed))            ; no-op: nothing to persist
        (do (deliver promise (:s computed)) :ok)
        :else
        ;; Phase 2: persist-then-publish. The append is the only realistic
        ;; thrower, and an I/O failure here is the serious case → HALT.
        (try
          ((.-append core) (:patch computed))       ; 1. durable
          (reset! state (:s' computed))             ; 2. publish to readers
          (deliver promise (:s' computed))          ; 3. ack the caller
          :ok
          (catch Throwable io-err
            (deliver promise io-err)
            io-err))))))

;; ---------------------------------------------------------------------------
;; Halt (§5)
;; ---------------------------------------------------------------------------

(defn- halt!
  "Flip the core into failed mode in one atomic swap: set the error (first
  writer wins) and clear the queue, then reject every snapshotted item's
  promise with `err` so no awaiting caller blocks forever. `deliver` is
  at-most-once, so re-rejecting an already-acked in-flight head is a no-op.
  With the error set, `enqueue!` refuses new work in the same swap that would
  have admitted it — there is no window in which a submit can be stranded."
  [^Core core ^Throwable err]
  (let [[old _] (swap-vals! (.-dispatch core)
                            (fn [{:keys [error] :as d}]
                              (assoc d :queue empty-queue
                                       :error (or error err))))]
    (doseq [{:keys [promise]} (:queue old)]
      (deliver promise err))))

;; ---------------------------------------------------------------------------
;; The drainer (single on-demand virtual thread; §2)
;; ---------------------------------------------------------------------------

(defn- drain-loop
  "Walk the queue to empty, then exit. The in-flight item stays at the head
  until `process!` completes, so the queue doubles as the drainer-liveness
  flag: the pop that yields an empty queue is, atomically, the decision to
  retire — a concurrent `conj` either lands before the pop (drainer continues)
  or after it onto an empty queue (submitter spawns the next drainer). Stops
  and rejects the rest on HALT (§5)."
  [^Core core]
  (let [dispatch (.-dispatch core)]
    (loop []
      (let [item (peek (:queue @dispatch))          ; head = in-flight
            sig  (process! core item)]
        (if (identical? :ok sig)
          (let [[_ new] (swap-vals! dispatch #(update % :queue pop))]
            (when (seq (:queue new))
              (recur)))
          (halt! core sig))))))                     ; sig is the I/O Throwable

(defn- start-drainer!
  "Spawn the drainer on a named virtual thread. An escaped Throwable from the
  loop plumbing itself becomes a HALT (rejecting all pending, in-flight head
  included) instead of a silently dead drainer with a stuck queue."
  [^Core core]
  (-> (Thread/ofVirtual)
      (.name "dj.recorder.dispatch/drainer")
      (.start (fn []
                (try
                  (drain-loop core)
                  (catch Throwable t
                    (halt! core t)))))))

;; ---------------------------------------------------------------------------
;; Enqueue (shared by submit! and await)
;; ---------------------------------------------------------------------------

(defn- enqueue!
  "Atomically enqueue `item` unless the core is halted. Returns nil on success
  or the halting Throwable (item NOT enqueued). The submitter that performs
  the empty→non-empty transition owns spawning the drainer, so exactly one
  drainer exists per busy period."
  [^Core core item]
  (let [[old new] (swap-vals! (.-dispatch core)
                              (fn [{:keys [error] :as d}]
                                (if error
                                  d
                                  (update d :queue conj item))))]
    (or (:error new)
        (do (when (empty? (:queue old))             ; 0→1 edge: we spawn
              (start-drainer! core))
            nil))))

;; ---------------------------------------------------------------------------
;; Construction
;; ---------------------------------------------------------------------------

(defn make-core
  "Build a dispatch core over an initial (rehydrated) state and a durable
  `append` fn `(fn [patch])` (may throw on I/O failure → HALT). Returns a
  `Core`: `deref` it for the realized state, drive it with `submit!` / `await`,
  and inspect failure with `error`."
  [init-state append]
  (->Core (atom init-state)
          (atom {:queue empty-queue :error nil})
          append))
