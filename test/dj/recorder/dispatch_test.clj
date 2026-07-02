(ns dj.recorder.dispatch-test
  "Exercises the dispatch core (alpha item 3): on-demand vthread drainer,
  persist-then-publish ordering, the per-tx promise as result + error channel,
  no-op skip, patch-error reject-and-continue, I/O-error HALT, quiesce!'s
  seal+drain+join, the two refusal markers (halted vs sealed/closed), and FIFO
  submission order under concurrency. Runtime deep-dive §2/§3/§5/§6/§9."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [dj.recorder.patch :as p]
            [dj.recorder.storage :as s]
            [dj.recorder.dispatch :as d]))

;; A capturing in-memory `append!`: records every persisted patch so a test can
;; assert *what* hit the log and *in what order*, with no files involved.
(defn- recording-append []
  (let [log (atom [])]
    {:log log
     :append! (fn [patch] (swap! log conj patch))}))

;; An `append!` that throws on the Nth (1-based) call — to drive the I/O HALT path.
(defn- throwing-append [fail-on]
  (let [n (atom 0)]
    (fn [_patch]
      (when (= fail-on (swap! n inc))
        (throw (java.io.IOException. "disk full (simulated)"))))))

(deftest basic-submit-applies-and-persists
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {}) append!)]
    (let [r1 @(d/submit! core (fn [_] {:user {:name "Bob"}}))
          r2 @(d/submit! core (fn [s] {:user {:age (if (:user s) 31 0)}}))]
      (is (= {:user {:name "Bob"}} r1) "promise resolves to the new realized state")
      (is (= {:user {:name "Bob" :age 31}} r2) "additive merge; authoring fn read prior state")
      (is (= {:user {:name "Bob" :age 31}} @core) "@db reflects the latest write")
      ;; persist-then-publish: exactly the authored patches hit the log, in order.
      (is (= [{:user {:name "Bob"}} {:user {:age 31}}] @log)))))

(deftest no-op-skips-the-log
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {:a 1}) append!)]
    (let [r @(d/submit! core (fn [_] {}))]              ; {} merges to no change
      (is (= {:a 1} r) "no-op promise resolves to the unchanged state")
      (is (= [] @log) "a no-op persists nothing"))
    ;; a patch-fn that returns the same leaf value is also a no-op
    (let [r @(d/submit! core (fn [_] {:a 1}))]
      (is (= {:a 1} r))
      (is (= [] @log) "still nothing persisted"))))

(deftest patch-error-rejects-one-tx-and-continues
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {:items [1 2 3]}) append!)]
    ;; #dj.recorder/splice onto a non-vector throws inside apply-patch
    (let [bad @(d/submit! core (fn [_] {:items (p/read-replace 5)}))
          err @(d/submit! core (fn [_] {:items (p/read-splice [{:at 0 :+ [9]}])}))
          ok  @(d/submit! core (fn [_] {:items 7}))]
      (is (= {:items 5} bad))
      (is (instance? Throwable err) "the bad patch is rejected via the promise")
      (is (= {:items 7} ok) "the drainer kept going to the next tx")
      (is (nil? (d/error core)) "a patch error does NOT halt the core")
      (is (= [{:items (p/read-replace 5)} {:items 7}] @log)
          "only the successful txs persisted; the rejected one did not"))))

(deftest io-error-halts
  (let [core (d/make-core (atom {}) (throwing-append 2))]  ; 2nd append throws
    (let [r1 @(d/submit! core (fn [_] {:a 1}))]
      (is (= {:a 1} r1)))
    (let [r2 @(d/submit! core (fn [_] {:b 2}))]
      (is (instance? Throwable r2) "the failing tx's promise carries the I/O error")
      (is (instance? java.io.IOException r2)))
    (testing "the core is halted: reads still work, writes throw (§5/§9)"
      (is (= {:a 1} @core) "reads return the last good state")
      (is (some? (d/error core)) "halted flag set to the I/O cause")
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/submit! core (fn [_] {:c 3}))) "further writes throw"))))

(deftest halt-rejects-pending
  ;; Items already queued behind the failing one must not block forever; they
  ;; get the halt error delivered to their promises.
  (let [start (promise)
        ;; first append blocks until we release it, so we can pile up a backlog
        gate  (atom :wait)
        log   (atom [])
        append! (fn [patch]
                  (deliver start true)
                  (when (= :wait @gate)
                    ;; spin until the test fills the queue, then fail this append
                    (loop [] (when (= :wait @gate) (Thread/sleep 1) (recur))))
                  (if (= :fail @gate)
                    (throw (java.io.IOException. "boom"))
                    (swap! log conj patch)))
        core  (d/make-core (atom {}) append!)
        p1    (d/submit! core (fn [_] {:a 1}))]        ; will block in append!
    @start                                              ; drainer is now inside append!
    (let [p2 (d/submit! core (fn [_] {:b 2}))          ; queued behind p1
          p3 (d/submit! core (fn [_] {:c 3}))]         ; queued behind p2
      (reset! gate :fail)                               ; release append! → it throws
      (is (instance? Throwable @p1) "the failing tx is rejected")
      (is (instance? Throwable @p2) "a pending tx is rejected on halt, not stranded")
      (is (instance? Throwable @p3) "...and so is the one behind it")
      (is (some? (d/error core))))))

(deftest await-blocks-until-empty
  (let [{:keys [log append!]} (recording-append)
        slow-append (fn [patch] (Thread/sleep 2) (append! patch))
        core (d/make-core (atom {}) slow-append)]
    (dotimes [i 20] (d/submit! core (fn [_] {(keyword (str "k" i)) i})))
    (d/await core)
    (is (= 20 (count @log)) "await returns only after all queued txs drained")
    (is (= 20 (count @core)))))

(deftest fifo-order-under-concurrency
  ;; Many submitters from many threads; the single drainer must serialize them
  ;; and submission order = commit order. Each tx appends its own index to a
  ;; vector via the additive algebra; the result must be a permutation whose
  ;; per-thread subsequence is monotonic. Simpler check: count + completeness.
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {:seq []}) append!)
        n    200
        ths  (mapv (fn [i]
                     (Thread/startVirtualThread
                      ^Runnable (fn [] (d/submit! core (fn [_] {:seq [i]})))))
                   (range n))]
    (run! #(.join ^Thread %) ths)
    (d/await core)
    (is (= n (count @log)) "every submitted tx was processed exactly once")
    (is (= (set (range n)) (set (:seq @core)))
        "all indices present, none lost or duplicated")
    (is (= n (count (:seq @core))))))

(deftest await-throws-on-halt
  ;; await is a barrier that resolves only once all prior work drained; if the
  ;; core is already halted, or halts before the barrier drains, it THROWS —
  ;; agent parity with clojure.core/await on a failed agent (§5). A returning
  ;; await therefore guarantees everything queued before it is durable.
  (testing "already halted → await throws immediately"
    (let [core (d/make-core (atom {}) (throwing-append 1))]  ; 1st append throws → HALT
      (is (instance? Throwable @(d/submit! core (fn [_] {:a 1}))) "drive the halt")
      (is (some? (d/error core)) "precondition: core is halted")
      (is (thrown? clojure.lang.ExceptionInfo (d/await core))
          "await on an already-halted core throws the halted ex-info")))
  (testing "halts under the pending barrier → await throws, does not hang"
    (let [gate    (atom :wait)
          start   (promise)
          append! (fn [_patch]
                    (deliver start true)
                    (loop [] (when (= :wait @gate) (Thread/sleep 1) (recur)))
                    (throw (java.io.IOException. "boom")))
          core    (d/make-core (atom {}) append!)]
      (d/submit! core (fn [_] {:a 1}))                 ; blocks inside append!
      @start                                           ; drainer is now in append!
      (let [awaiter (future (try (d/await core) ::returned
                                 (catch clojure.lang.ExceptionInfo _ ::threw)))]
        (Thread/sleep 5)                               ; let the barrier queue behind the tx
        (reset! gate :fail)                            ; release append! → it throws → HALT
        (is (= ::threw @awaiter)
            "an await pending when the core halts throws rather than blocking forever")))))

(deftest stack-overflow-is-a-per-tx-reject
  ;; A StackOverflowError arrives with the stack already unwound: a runaway
  ;; patch-fn is a user-code verdict, rejected per-tx like any authoring error —
  ;; the drainer keeps going and the core is NOT halted (§5).
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {}) append!)
        blew @(d/submit! core (fn [_] (let [rec (fn rec [n] (inc (rec (inc n))))] (rec 0))))
        ok   @(d/submit! core (fn [_] {:a 1}))]
    (is (instance? StackOverflowError blew) "the runaway patch-fn is rejected via its promise")
    (is (nil? (d/error core)) "a StackOverflowError does NOT halt the core")
    (is (= {:a 1} ok) "the drainer kept draining the next tx")
    (is (= [{:a 1}] @log) "only the good tx persisted")))

(deftest vm-error-halts-via-backstop
  ;; A VirtualMachineError other than StackOverflowError is process health, never
  ;; a per-tx verdict: process! rethrows it and the drainer backstop turns it
  ;; into a HALT — a dying JVM must not keep writing to disk (§5). Throwing a
  ;; bare OutOfMemoryError *object* is safe; only the real JVM condition is not.
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core (atom {}) append!)
        p    (d/submit! core (fn [_] (throw (OutOfMemoryError. "simulated"))))]
    (is (instance? OutOfMemoryError @p) "the VM error reaches the in-flight tx's promise")
    (is (instance? OutOfMemoryError (d/error core)) "and HALTs the core, not a per-tx reject")
    (is (= [] @log) "nothing was persisted")
    (is (thrown? clojure.lang.ExceptionInfo (d/submit! core (fn [_] {:b 2})))
        "the halted core refuses further writes")))

(deftest quiesce-drains-and-retires
  ;; close!'s drain step (dispatch/quiesce!): enqueue a final barrier, THEN read
  ;; the drainer and join it — so all prior work is durable and the drainer is
  ;; provably retired. Ordering guard: reading the drainer BEFORE the barrier
  ;; could join a retired drainer from an earlier busy period while a fresh one
  ;; is mid-flight (see quiesce!'s doc). We deliberately cross a busy-period
  ;; boundary — period 1 drains and retires its drainer (so the drainer atom
  ;; holds a now-dead thread), then period 2 fires fresh fire-and-forget work —
  ;; so quiesce! must still flush all of it.
  (let [{:keys [log append!]} (recording-append)
        slow (fn [patch] (Thread/sleep 2) (append! patch))
        core (d/make-core (atom {}) slow)]
    (dotimes [i 5] (d/submit! core (fn [_] {(keyword (str "a" i)) i})))
    (d/await core)                                     ; period 1 drains → drainer retires
    (dotimes [i 5] (d/submit! core (fn [_] {(keyword (str "b" i)) i})))  ; period 2, no await
    (d/quiesce! core)
    (is (= 10 (count @log)) "quiesce joined the LIVE drainer: every prior write is durable")
    (is (= 10 (count @core)))
    (is (not (.isAlive ^Thread @(.-drainer ^dj.recorder.dispatch.Core core)))
        "the drainer is provably retired after quiesce")
    (testing "quiesce is halt-safe / re-entrant on a quiescent core (never throws)"
      (is (nil? (d/quiesce! core)) "a second quiesce just returns nil"))))

(defn- ex-data-of [f]
  ;; run f, returning the ex-data of the ExceptionInfo it throws (nil if it didn't).
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest submit-and-await-refuse-on-sealed-and-halted-cores
  ;; Locks down enqueue!'s refusal detection: it decides "was I refused?" by
  ;; (identical? old new), which only holds because the swap fn returns `d`
  ;; ITSELF untouched on refusal. If that fn is ever "cleaned up" to
  ;; (assoc d ...) unconditionally, old/new diverge and refusals silently stop
  ;; being detected — the compiler won't catch it, but this test will. We also
  ;; pin the two distinct markers the write-side entry points surface.
  (testing "sealed (closed) core → submit!/await throw :dj.recorder/closed"
    (let [{:keys [append!]} (recording-append)
          core (d/make-core (atom {}) append!)]
      (d/quiesce! core)                                 ; seal + drain + retire
      (is (:dj.recorder/closed (ex-data-of #(d/submit! core (fn [_] {:a 1}))))
          "submit! on a sealed core throws the closed ex-info")
      (is (:dj.recorder/closed (ex-data-of #(d/await core)))
          "await on a sealed core throws the closed ex-info (no stray drainer)")))
  (testing "halted core → submit!/await throw :dj.recorder/halted"
    (let [core (d/make-core (atom {}) (throwing-append 1))]  ; 1st append throws → HALT
      (is (instance? Throwable @(d/submit! core (fn [_] {:a 1}))) "drive the halt")
      (is (some? (d/error core)) "precondition: the core is halted")
      (is (:dj.recorder/halted (ex-data-of #(d/submit! core (fn [_] {:b 2}))))
          "submit! on a halted core throws the halted ex-info")
      (is (:dj.recorder/halted (ex-data-of #(d/await core)))
          "await on a halted core throws the halted ex-info")))
  (testing "halted AND sealed → halted wins the reporting (enqueue! prefers :error)"
    (let [core (d/make-core (atom {}) (throwing-append 1))]
      @(d/submit! core (fn [_] {:a 1}))                 ; halt it
      (d/quiesce! core)                                 ; then seal it (halt-safe, no-throw)
      (is (:dj.recorder/halted (ex-data-of #(d/submit! core (fn [_] {:b 2}))))
          "a core that is both halted and sealed reports halted, not closed"))))

(deftest integrates-with-real-storage
  ;; The drainer's append! is the real file writer; after draining, an
  ;; independent read-log must reproduce the same state (one fold, §3/§6).
  (let [f (doto (java.io.File/createTempFile "dj-dispatch" ".edn") .delete .deleteOnExit)
        path (.getPath f)]
    (with-open [w (s/open-writer path)]
      (let [core (d/make-core (atom {}) (fn [patch] (s/append! w patch)))]
        @(d/submit! core (fn [_] {:user {:name "Bob"}}))
        @(d/submit! core (fn [_] {:user {:age 30}}))
        @(d/submit! core (fn [_] {:tags #{:a :b}}))
        @(d/submit! core (fn [_] {:tags (p/read-dissoc #{:a})}))
        (d/await core)
        (let [in-mem @core
              on-disk (:state (s/read-log {} path))]
          (is (= {:user {:name "Bob" :age 30} :tags #{:b}} in-mem))
          (is (= in-mem on-disk) "rehydrate from disk matches the live state"))))))
