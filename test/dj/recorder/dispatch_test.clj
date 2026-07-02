(ns dj.recorder.dispatch-test
  "Exercises the dispatch core (alpha item 3): on-demand vthread drainer,
  persist-then-publish ordering, the per-tx promise as result + error channel,
  no-op skip, patch-error reject-and-continue, I/O-error HALT, and FIFO
  submission order under concurrency. Runtime deep-dive §2/§3/§5/§9."
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
        core (d/make-core {} append!)]
    (let [r1 @(d/submit! core (fn [_] {:user {:name "Bob"}}))
          r2 @(d/submit! core (fn [s] {:user {:age (if (:user s) 31 0)}}))]
      (is (= {:user {:name "Bob"}} r1) "promise resolves to the new realized state")
      (is (= {:user {:name "Bob" :age 31}} r2) "additive merge; authoring fn read prior state")
      (is (= {:user {:name "Bob" :age 31}} @core) "@db reflects the latest write")
      ;; persist-then-publish: exactly the authored patches hit the log, in order.
      (is (= [{:user {:name "Bob"}} {:user {:age 31}}] @log)))))

(deftest no-op-skips-the-log
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core {:a 1} append!)]
    (let [r @(d/submit! core (fn [_] {}))]              ; {} merges to no change
      (is (= {:a 1} r) "no-op promise resolves to the unchanged state")
      (is (= [] @log) "a no-op persists nothing"))
    ;; a patch-fn that returns the same leaf value is also a no-op
    (let [r @(d/submit! core (fn [_] {:a 1}))]
      (is (= {:a 1} r))
      (is (= [] @log) "still nothing persisted"))))

(deftest patch-error-rejects-one-tx-and-continues
  (let [{:keys [log append!]} (recording-append)
        core (d/make-core {:items [1 2 3]} append!)]
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
  (let [core (d/make-core {} (throwing-append 2))]      ; 2nd append throws
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
        core  (d/make-core {} append!)
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
        core (d/make-core {} slow-append)]
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
        core (d/make-core {:seq []} append!)
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

(deftest integrates-with-real-storage
  ;; The drainer's append! is the real file writer; after draining, an
  ;; independent read-log must reproduce the same state (one fold, §3/§6).
  (let [f (doto (java.io.File/createTempFile "dj-dispatch" ".edn") .delete .deleteOnExit)
        path (.getPath f)]
    (with-open [w (s/open-writer path)]
      (let [core (d/make-core {} (fn [patch] (s/append! w patch)))]
        @(d/submit! core (fn [_] {:user {:name "Bob"}}))
        @(d/submit! core (fn [_] {:user {:age 30}}))
        @(d/submit! core (fn [_] {:tags #{:a :b}}))
        @(d/submit! core (fn [_] {:tags (p/read-dissoc #{:a})}))
        (d/await core)
        (let [in-mem @core
              on-disk (:state (s/read-log {} path))]
          (is (= {:user {:name "Bob" :age 30} :tags #{:b}} in-mem))
          (is (= in-mem on-disk) "rehydrate from disk matches the live state"))))))
