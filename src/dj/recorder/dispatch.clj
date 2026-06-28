(ns dj.recorder.dispatch
  "The dispatch core for dj.recorder — alpha item 3.

  A single on-demand virtual thread (the *drainer*) walks a queue of pending
  transactions, applying each in submission order. The drainer is spawned when
  work arrives and exits when the queue drains (north star: \"a single virtual
  thread walks the queue\"; runtime deep-dive §2). At most one drainer is alive
  at a time, so transactions form a total order with no per-item locking and a
  race-free read-modify-write (the authoring fn runs serialized, agent-style —
  §9).

  Contracts pinned from the runtime deep-dive:
    §2  on-demand vthread drainer, lock-free enqueue, drain-then-recheck guard.
    §3  PERSIST-THEN-PUBLISH: append the patch durably *first*, then publish to
        readers (`a-state`), then ack the caller. `@db` never shows state that
        isn't on disk.
    §5  Two failure classes via the per-tx promise:
          - authoring/patch error (patch-fn or apply-patch throws): reject just
            this tx (promise -> Throwable), state untouched, drainer continues.
          - I/O append error: HALT — flip the core into a failed mode that
            rejects further submits (writes throw), reject all pending txs with
            the I/O error, and stop the drainer. Reads still work; recover via
            close! + re-open (item 4).

  Pure dispatch + the injected `append!`; no file knowledge of its own (storage
  is item 2, the public API/lifecycle is item 4). `append!` is `(fn [patch])`
  that durably persists one patch and may throw on I/O failure."
  (:require [dj.recorder.patch :as patch]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Core construction
;; ---------------------------------------------------------------------------

(defn make-core
  "Build a dispatch core over an initial (rehydrated) state and a durable
  `append!` fn `(fn [patch])` (may throw on I/O failure → HALT). Returns a map
  of the core's atoms plus `append!`; drive it with `submit!` / `await-drained`
  and read it with `state` / `halted`."
  [init-state append!]
  {:a-state     (atom init-state)
   :a-queue     (atom clojure.lang.PersistentQueue/EMPTY)
   :a-draining? (atom false)
   :a-halted    (atom nil)        ; nil = live; a Throwable = halted (the cause)
   :append!     append!})

(defn state
  "Current realized state — what `@db` reads. Persist-then-publish (§3): this is
  the last *durably-written* state and may lag the queue. Never blocks on I/O."
  [core]
  @(:a-state core))

(defn halted
  "The I/O Throwable that halted this core, or nil if it is still live (§5)."
  [core]
  @(:a-halted core))

;; ---------------------------------------------------------------------------
;; Per-transaction processing (persist-then-publish; §3 / §5)
;; ---------------------------------------------------------------------------

(defn- process!
  "Run one queued item `{:patch-fn :promise}`. Returns `:ok` to keep draining or
  `:halt` to stop (I/O failure; the core's halted flag is set first).

  Authoring/patch errors reject only this tx and return `:ok` — a bad patch can
  neither poison the db nor kill the drainer (§5). The authoring fn reads the
  latest realized state here, on the single drainer thread, so read-modify-write
  is race-free (§9)."
  [core {:keys [patch-fn promise]}]
  (let [a-state (:a-state core)
        ;; Phase 1: author + apply, off the current realized state. Any throw
        ;; here is a patch/authoring error: reject this tx, leave state alone.
        computed (try
                   (let [s     @a-state
                         p     (patch-fn s)
                         s'    (patch/apply-patch s p)]
                     {:s s :patch p :s' s'})
                   (catch Throwable e
                     (deliver promise e)
                     nil))]
    (cond
      (nil? computed) :ok                           ; rejected above; continue
      (= (:s' computed) (:s computed))              ; no-op: nothing to persist
      (do (deliver promise (:s computed)) :ok)
      :else
      ;; Phase 2: persist-then-publish. The append is the only realistic
      ;; thrower, and an I/O failure here is the serious case → HALT.
      (try
        ((:append! core) (:patch computed))         ; 1. durable
        (reset! a-state (:s' computed))             ; 2. publish to readers
        (deliver promise (:s' computed))            ; 3. ack the caller
        :ok
        (catch Throwable io-err
          (reset! (:a-halted core) io-err)
          (deliver promise io-err)
          :halt)))))

(defn- reject-pending!
  "Deliver `err` to every still-queued item's promise and clear the queue, so a
  HALT doesn't leave awaiting callers blocked forever. Called only after the
  halted flag is set (so `submit!` is already refusing new work)."
  [core err]
  (loop []
    (when-let [item (peek @(:a-queue core))]
      (deliver (:promise item) err)
      (swap! (:a-queue core) pop)
      (recur))))

;; ---------------------------------------------------------------------------
;; The drainer (single on-demand virtual thread; §2)
;; ---------------------------------------------------------------------------

(defn- drain-loop
  "Walk the queue to empty, then exit — clearing the draining flag with a
  drain-then-recheck guard so an item that lands in the window between
  \"queue empty\" and \"flag cleared\" can re-spawn a drainer instead of being
  stranded (§2). Stops early and rejects the rest on HALT (§5)."
  [core]
  (let [a-queue     (:a-queue core)
        a-draining? (:a-draining? core)]
    (loop []
      (if-let [item (peek @a-queue)]
        (let [sig (process! core item)]
          (swap! a-queue pop)                       ; item done; remove it
          (if (= sig :halt)
            (reject-pending! core (halted core))    ; drop the rest, stop
            (recur)))
        (do (reset! a-draining? false)
            ;; race guard: a submit! may have conj'd + lost the CAS in the
            ;; window above. If so, reclaim the drainer role and keep going.
            (when (and (seq @a-queue)
                       (compare-and-set! a-draining? false true))
              (recur)))))))

(defn- start-drainer! [core]
  (Thread/startVirtualThread ^Runnable (fn [] (drain-loop core))))

;; ---------------------------------------------------------------------------
;; Submission + await
;; ---------------------------------------------------------------------------

(defn submit!
  "Enqueue a `state → patch` authoring fn and ensure a drainer is running.
  Returns the per-tx promise (§ runtime A6): it resolves to the new realized
  state on success, or to a `Throwable` on a patch/authoring or I/O error (§5).
  Submission order is commit order (lock-free `conj`; §2).

  Throws if the core is halted — writes throw on a halted db (§5/§9); reads
  (`state`) still work. Recover via the public lifecycle's close!/re-open."
  [core patch-fn]
  (when-let [err (halted core)]
    (throw (ex-info "dj.recorder: db is halted by an earlier I/O failure; writes throw, reads still work — close! and re-open to recover"
                    {:dj.recorder/halted true} err)))
  (let [p    (promise)
        item {:patch-fn patch-fn :promise p}]
    (swap! (:a-queue core) conj item)               ; lock-free MPSC enqueue
    ;; Spawn a drainer iff none is running; the CAS lets exactly one win, so
    ;; the empty→non-empty edge is the only spawn trigger (§2).
    (when (compare-and-set! (:a-draining? core) false true)
      (start-drainer! core))
    p))

(defn await-drained
  "Block until every tx queued *before this call* has been processed
  (agent-style `await`). Implemented as a no-op barrier: a patch that yields the
  unchanged state, enqueued behind the current work and processed in FIFO order,
  so its promise resolving means all prior txs drained. Returns nil. On a halted
  core, returns immediately (nothing will drain)."
  [core]
  (when-not (halted core)
    @(submit! core (fn [_] {})))                    ; {} merges to a no-op (§3)
  nil)
