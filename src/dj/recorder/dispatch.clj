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
        shows state that isn't on disk. The failure-scope try covers exactly
        the append and nothing after it: once `append` returns, the patch is
        on disk and nothing may report otherwise (no false-negative acks — a
        caller must never be told a durable write failed, or a re-open retry
        would double-apply it).
    §5  Failure classes, decided by *phase*, not by exception type (`append`
        is injected and may throw anything; the phase says what was at risk):
          - authoring/patch error (patch-fn or apply-patch throws): reject
            just this tx (promise -> Throwable), state untouched, drainer
            continues. Includes StackOverflowError (by catch time the stack
            has unwound; a runaway patch-fn is a user-code verdict).
          - I/O append error: HALT — set the core's error, reject all pending
            txs with it, stop the drainer, and refuse further submits (writes
            throw). Reads still work; recover via close! + re-open (item 4).
          - VirtualMachineError (other than StackOverflowError): process
            health, never a tx verdict. Always escalates to HALT via the
            drainer's backstop, regardless of phase.
        An escaped Throwable from the drain loop itself is likewise a HALT
        rather than a silently dead drainer.
    §7  INTERRUPTS ARE NOT A CONTROL CHANNEL. The queue is the drainer's only
        control channel; nothing may ever interrupt the drainer thread.
        Shutdown (item 4's close!) is expressed through the queue — enqueue a
        final barrier and join the drainer (see the `drainer` field). Under
        this invariant an InterruptedException escaping a patch-fn can only
        mean the patch-fn did its own thread plumbing, so it is treated as an
        ordinary per-tx authoring error. If a future item wants interruption
        as cancellation, this section must be redesigned first.

  Pure dispatch + the injected `append` fn; no file knowledge of its own
  (storage is item 2, the public API/lifecycle is item 4). `append` is
  `(fn [patch])` that durably persists one patch and may throw on I/O failure.

  Per-tx promises resolve to the new realized state on success or to a
  `Throwable` on failure — deliberately, at this layer: the drainer must never
  throw across the promise boundary, and the public wrapper (item 4) owns the
  caller-facing ergonomics (deref-throws / CompletableFuture / variant map).
  Pinned non-goal: a legitimate state value is never itself a Throwable."
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
    all prior txs drained. Returns nil. Throws the halted ex-info (same shape
    as `submit!`, cause = the halting Throwable) if the core is already halted
    OR halts before the barrier drains — matching `clojure.core/await`, which
    throws on a failed agent. `await` returning nil therefore *guarantees*
    everything queued before it is durable.")
  (error [core]
    "The Throwable that halted this core (I/O failure, VM error, or a dispatch
    bug caught by the drainer backstop — §5), or nil if it is still live.
    Named after `agent-error`."))

(declare enqueue! halted-ex)

(deftype Core [state     ; atom: last durably-written state (what deref shows)
               dispatch  ; atom: {:queue PersistentQueue, :error nil|Throwable}
               append    ; (fn [patch]): durable persist; may throw → HALT
               drainer]  ; atom: most recently spawned drainer Thread — for
                         ; item 4's close! to join after its final barrier
                         ; (never interrupt; §7). nil until first submit.
  IDeref
  ;; Current realized state — what `@db` reads. Persist-then-publish (§3): the
  ;; last *durably-written* state; may lag the queue. Never blocks on I/O.
  (deref [_] @state)

  IRecorderCore
  (submit! [core patch-fn]
    (let [p (promise)]
      (if-let [err (enqueue! core {:patch-fn patch-fn :promise p})]
        (throw (halted-ex err))
        p)))

  (await [core]
    (let [p (promise)]
      (if-let [err (enqueue! core {:barrier? true :promise p})]
        (throw (halted-ex err))                    ; already halted (agent parity)
        (let [r @p]
          (if (instance? Throwable r)
            (throw (halted-ex r))                  ; halted under the barrier
            nil)))))

  (error [_]
    (:error @dispatch)))

;; ---------------------------------------------------------------------------
;; Halted-core error (shared by submit! and await so the shape can't drift)
;; ---------------------------------------------------------------------------

(defn- halted-ex
  "The ex-info thrown by any write-side entry point observing a halted core.
  `err` (the halting Throwable) rides along as the cause."
  [^Throwable err]
  (ex-info "dj.recorder: db is halted by an earlier failure; writes throw, reads still work — close! and re-open to recover"
           {:dj.recorder/halted true}
           err))

;; ---------------------------------------------------------------------------
;; Per-transaction processing (persist-then-publish; §3 / §5)
;; ---------------------------------------------------------------------------

(defn- process!
  "Run one queued item. Returns `:ok` to keep draining, or a Throwable to
  signal HALT. Barrier items just ack and continue. Authoring/patch errors
  reject only this tx and return `:ok` — a bad patch can neither poison the db
  nor kill the drainer (§5) — EXCEPT VirtualMachineError (minus SOE), which is
  rethrown for the backstop to turn into HALT: a dying JVM must not keep
  writing to disk. The authoring fn reads the latest realized state here, on
  the single drainer thread, so read-modify-write is race-free (§9)."
  [^Core core {:keys [patch-fn promise barrier?]}]
  (if barrier?
    (do (deliver promise nil) :ok)
    (let [state (.-state core)
          ;; Phase 1: author + apply, off the current realized state. A throw
          ;; here is a patch/authoring error scoped to this tx alone —
          ;; process-health errors excepted (§5).
          computed (try
                     (let [s  @state
                           p  (patch-fn s)
                           s' (patch/apply-patch s p)]
                       {:s s :patch p :s' s'})
                     (catch StackOverflowError e   ; unwound ⇒ user-code verdict
                       (deliver promise e)
                       nil)
                     (catch VirtualMachineError e  ; process health ⇒ backstop HALT
                       (throw e))
                     (catch Throwable e
                       (deliver promise e)
                       nil))]
      (cond
        (nil? computed) :ok                         ; rejected above; continue
        (= (:s' computed) (:s computed))            ; no-op: nothing to persist
        (do (deliver promise (:s computed)) :ok)
        :else
        ;; Phase 2: persist-then-publish. The try covers EXACTLY the append
        ;; (§3): once `append` returns, the patch is durable and this tx can
        ;; only be acked as a success. reset!/deliver below cannot throw short
        ;; of a VM error, which then escapes to the backstop as a process-
        ;; health HALT — honest, since the write itself did not fail.
        (let [io-err (try ((.-append core) (:patch computed)) nil
                          (catch Throwable e e))]
          (if io-err
            (do (deliver promise io-err) io-err)    ; HALT (§5)
            (do (reset! state (:s' computed))       ; 2. publish to readers
                (deliver promise (:s' computed))    ; 3. ack the caller
                :ok)))))))

;; ---------------------------------------------------------------------------
;; Halt (§5)
;; ---------------------------------------------------------------------------

(defn- halt!
  "Flip the core into failed mode in one atomic swap: set the error (first
  writer wins) and clear the queue, then reject every snapshotted item's
  promise with `err` so no awaiting caller blocks forever. The error is set
  *before* any deliver, so even a mid-halt! death (e.g. OOM while rejecting)
  leaves the core refusing new work. `deliver` is at-most-once, so
  re-rejecting an already-acked in-flight head is a no-op. Drainer-private:
  only the drainer thread (loop or backstop) may call this — external callers
  would race the pop/peek cycle in `drain-loop` (which is guarded, but by
  design the queue is the only cross-thread channel; §7)."
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
  and rejects the rest on HALT (§5). The `when-let` makes the loop total: a
  head that vanished (only possible if the halt invariant were ever violated)
  retires the drainer instead of processing nil."
  [^Core core]
  (let [dispatch (.-dispatch core)]
    (loop []
      (when-let [item (peek (:queue @dispatch))]    ; head = in-flight
        (let [sig (process! core item)]
          (if (identical? :ok sig)
            (let [[_ new] (swap-vals! dispatch #(update % :queue pop))]
              (when (seq (:queue new))
                (recur)))
            (halt! core sig)))))))                  ; sig is the halting Throwable

(defn- start-drainer!
  "Spawn the drainer on a named virtual thread and stash it on the core (for
  item 4's close! to join — never interrupt; §7). An escaped Throwable from
  the loop plumbing itself — including a rethrown VirtualMachineError from
  process! — becomes a HALT (rejecting all pending, in-flight head included)
  instead of a silently dead drainer with a stuck queue."
  [^Core core]
  (let [t (-> (Thread/ofVirtual)
              (.name "dj.recorder.dispatch/drainer")
              (.start (fn []
                        (try
                          (drain-loop core)
                          (catch Throwable t
                            (halt! core t))))))]
    (reset! (.-drainer core) t)))

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
;; Quiesce — the public lifecycle's close! shutdown step (§6/§7)
;; ---------------------------------------------------------------------------

(defn quiesce!
  "Bring the drainer to rest for item 4's close! (§6/§7): enqueue a final barrier
  — or observe the core already halted, in which case nothing is left to drain —
  then JOIN the drainer thread so it is provably retired before the caller closes
  the writer and releases the single-writer lock. Never interrupts the drainer
  (§7). Returns nil.

  Order is load-bearing: the barrier is enqueued *before* `drainer` is read. That
  atom holds the most recently spawned thread; reading it first could join a
  drainer that already retired at the end of an earlier busy period while the
  barrier we just enqueued spawns a fresh drainer that is still mid-flight —
  joining the dead one and returning while live work runs. Enqueue-then-read pins
  the drainer that owns our barrier (or a live successor); joining it subsumes
  waiting on the barrier promise, since the drainer only dies after processing
  every item up to and including that barrier.

  Halt-safe (does not throw): a halted core's drainer is already retiring
  (`halt!` cleared the queue) and joins at once; the barrier's own promise is
  never read (the join *is* the barrier), so the halted-vs-live distinction and
  await's throw-on-halt are both moot here. Intended for one-shot shutdown, with
  no concurrent submits racing it (close! guards that with its closed flag)."
  [^Core core]
  (enqueue! core {:barrier? true :promise (promise)}) ; halt err ignored: nothing left to drain
  (when-let [^Thread t @(.-drainer core)]
    (.join t))
  nil)

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
          append
          (atom nil)))
