(ns dj.recorder.adapter.concurrency-test
  "Smoke test for the opt-in dj.concurrency ResultStore adapter
  (`dj.recorder.adapter.concurrency`). It requires dj.concurrency on the
  classpath, so it lives OUTSIDE the core `test/` suite and runs only via the
  :adapter alias — deliberately light and infrequent:

    clojure -M:adapter

  Scope: the adapter satisfies dj.concurrency's ResultStore contract over a real
  db — miss=nil vs hit=envelope (incl. the {:result nil} case the envelope
  exists for), synchronous-durable record!, replace-on-re-record, evict!, and
  survival across close+reopen. The store's own semantics are dj.concurrency's
  to test; here we only prove the adapter wires them to dj.recorder correctly."
  (:require [clojure.test :refer [deftest is]]
            [dj.recorder :as r]
            [dj.concurrency.store :as store]
            [dj.recorder.adapter.concurrency :as adapter]))

(defn- tmp-path []
  (let [f (java.io.File/createTempFile "dj-recorder-adapter" ".edn")]
    (.delete f)
    (.deleteOnExit f)
    (.getPath f)))

(deftest recorder-store-satisfies-resultstore
  (let [path (tmp-path)
        db   (r/open path)
        s    (adapter/recorder-store db [:results "run-1"])]
    (try
      (is (nil? (store/lookup s [:summarize "a"])) "unknown key is a miss (nil)")

      ;; record! blocks until durable, then lookup returns the envelope verbatim
      (is (nil? (store/record! s [:summarize "a"] {:result 42})) "record! returns nil")
      (is (= {:result 42} (store/lookup s [:summarize "a"])) "hit returns the envelope")

      ;; the envelope's whole reason to exist: a nil result is a HIT, not a miss
      (store/record! s [:summarize "b"] {:result nil})
      (is (= {:result nil} (store/lookup s [:summarize "b"]))
          "a {:result nil} envelope round-trips — hit is distinguishable from miss")

      ;; re-record REPLACES the envelope (leaf-patch wraps it in #replace), not merge
      (store/record! s [:summarize "a"] {:result 43})
      (is (= {:result 43} (store/lookup s [:summarize "a"])) "re-record replaces the envelope")

      ;; evict! removes it, durably
      (is (nil? (store/evict! s [:summarize "a"])) "evict! returns nil")
      (is (nil? (store/lookup s [:summarize "a"])) "evicted key is a miss again")

      ;; path-prefix namespacing: a different run doesn't see run-1's entries
      (is (nil? (store/lookup (adapter/recorder-store db [:results "run-2"]) [:summarize "b"]))
          "a different path-prefix is an independent namespace")
      (finally (r/close! db)))

    ;; durability: reopen the same log; the surviving envelope is still there
    (let [db2 (r/open path)]
      (try
        (is (= {:result nil}
               (store/lookup (adapter/recorder-store db2 [:results "run-1"]) [:summarize "b"]))
            "records survive close + reopen (durable, persist-then-publish)")
        (finally (r/close! db2))))))
