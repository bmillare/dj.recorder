(ns dj.recorder.storage-test
  "Exercises the durable substrate: append/replay round-trip, the torn-tail
  contract (surface, never silently drop — runtime deep-dive §4), discard via
  truncate, ambient-print-length safety, and the single-writer file lock."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [dj.recorder.patch :as p]
            [dj.recorder.storage :as s])
  (:import [java.nio.file Files]
           [java.nio.channels OverlappingFileLockException]
           [java.util.concurrent.locks Lock]))

(defn- tmp-path
  "A unique temp path with no file present yet (so writer/open create it)."
  []
  (let [f (java.io.File/createTempFile "dj-recorder-storage" ".edn")]
    (.delete f)
    (.deleteOnExit f)
    (.getPath f)))

(defn- file-bytes ^long [path]
  (alength ^bytes (Files/readAllBytes (.toPath (io/file path)))))

(deftest round-trip-clean-log
  (let [path    (tmp-path)
        patches [{:user {:name "Bob"}}
                 {:user {:age 30}}
                 {:user (p/read-replace {:name "Alice"})}
                 {:tags #{:a :b}}
                 {:tags (p/read-dissoc #{:a})}
                 {:items [1 2 3]}
                 {:items (p/read-splice [{:at 1 :+ [:x]}])}]]
    (with-open [w (s/open-writer path)]
      (doseq [pt patches] (s/append! w pt)))
    (let [res (s/read-log {} path)]
      (is (nil? (:torn-tail res)) "a cleanly-terminated log has no torn tail")
      (is (= (count patches) (:count res)))
      (is (= (p/rehydrate {} patches) (:state res))
          "replay matches the pure-algebra fold of the same patches")
      (is (= {:user {:name "Alice"} :tags #{:b} :items [1 :x 2 3]}
             (:state res))
          "markers round-trip through the actual file path"))))

(deftest missing-file-is-baseline
  (let [path (tmp-path)]                         ; tmp-path deleted the file
    (is (= {:state {:seed 1} :count 0 :torn-tail nil}
           (s/read-log {:seed 1} path)))))

(deftest empty-file-is-baseline
  (let [f (java.io.File/createTempFile "dj-recorder-empty" ".edn")]
    (.deleteOnExit f)
    (let [res (s/read-log {} (.getPath f))]
      (is (nil? (:torn-tail res)))
      (is (= 0 (:count res)))
      (is (= {} (:state res))))))

(deftest torn-tail-surfaced
  (let [path (tmp-path)]
    (with-open [w (s/open-writer path)]
      (s/append! w {:a 1})
      (s/append! w {:b 2}))
    ;; simulate a crash mid-append: a partial final record with no trailing \n
    (spit path "{:c 3" :append true)
    (let [res (s/read-log {} path)]
      (is (= {:a 1 :b 2} (:state res)) "good prefix is fully replayed")
      (is (= 2 (:count res)))
      (let [tt (:torn-tail res)]
        (is (some? tt) "the torn tail is surfaced, not dropped")
        (is (= "{:c 3" (:text tt)))
        (is (= (- (file-bytes path) 5) (:offset tt))
            "offset points just past the last good newline")))))

(deftest torn-tail-need-not-parse
  ;; the partial is never parsed — even unreadable garbage is surfaced as-is.
  (let [path (tmp-path)]
    (with-open [w (s/open-writer path)]
      (s/append! w {:a 1}))
    (spit path "{:c 3 :open [#unbalanced" :append true)
    (let [res (s/read-log {} path)]
      (is (= {:a 1} (:state res)))
      (is (= "{:c 3 :open [#unbalanced" (:text (:torn-tail res)))))))

(deftest whole-file-torn
  ;; a single record that never got its newline: nothing committed, all torn.
  (let [path (tmp-path)]
    (spit path "{:a 1")
    (let [res (s/read-log {:base 0} path)]
      (is (= {:base 0} (:state res)))
      (is (= 0 (:count res)))
      (is (= 0 (:offset (:torn-tail res))))
      (is (= "{:a 1" (:text (:torn-tail res)))))))

(deftest truncate-discards-torn-tail
  (let [path (tmp-path)]
    (with-open [w (s/open-writer path)]
      (s/append! w {:a 1}))
    (spit path "{:partial" :append true)
    (s/truncate-to path (:offset (:torn-tail (s/read-log {} path))))
    (let [res (s/read-log {} path)]
      (is (nil? (:torn-tail res)) "torn tail gone after truncate")
      (is (= {:a 1} (:state res)) "intact prefix preserved")
      (is (= 1 (:count res))))))

(deftest append-ignores-ambient-print-length
  ;; a caller's *print-length* must never truncate a record into `...`.
  (let [path (tmp-path)
        big  {:xs (vec (range 50))}]
    (binding [*print-length* 3
              *print-level*  1]
      (with-open [w (s/open-writer path)]
        (s/append! w big)))
    (is (= big (:state (s/read-log {} path)))
        "the full structure persists despite ambient print limits")))

(deftest file-lock-mechanics
  (let [path (tmp-path)
        lock (s/file-lock path)]
    (.lock ^Lock lock)
    (is (.exists (io/file (str path ".lock"))) ".lock marker written")
    (is (.exists (io/file (str path ".lock_status"))) ".lock_status sidecar created")
    (testing "a second overlapping lock in the same JVM is refused"
      (let [lock2 (s/file-lock path)]
        (is (thrown? OverlappingFileLockException (.lock ^Lock lock2)))))
    (.unlock ^Lock lock)
    (is (not (.exists (io/file (str path ".lock"))))
        ".lock marker removed on unlock")
    (is (.exists (io/file (str path ".lock_status")))
        ".lock_status kept on unlock (split-brain safety)")
    (io/delete-file (str path ".lock_status") true)))
