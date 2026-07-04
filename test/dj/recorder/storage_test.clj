(ns dj.recorder.storage-test
  "Exercises the durable substrate: append/replay round-trip, the write-side
  EDN round-trip guard (a non-EDN patch is refused, log untouched), the
  torn-tail contract (surface, never silently drop — runtime deep-dive §4),
  discard via truncate (with its out-of-bounds guard), ambient-print safety,
  and the fail-fast single-writer file lock (distinct in-process/other-process
  errors, tryLock, idempotent unlock)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [dj.recorder.patch :as p]
            [dj.recorder.storage :as s]
            [dj.recorder.protocols :as proto])
  (:import [java.nio.file Files]
           [java.util.concurrent TimeUnit]
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
      (doseq [pt patches] (proto/append! w pt)))
    (let [res (s/read-log {} path)]
      (is (nil? (:torn-tail res)) "a cleanly-terminated log has no torn tail")
      (is (= (count patches) (:count res)))
      (is (= (p/rehydrate {} patches) (:state res))
          "replay matches the pure-algebra fold of the same patches")
      (is (= {:user {:name "Alice"} :tags #{:b} :items [1 :x 2 3]}
             (:state res))
          "markers round-trip through the actual file path"))))

(deftest ensure-parent-dir-creates-missing-subdirs
  ;; A first-run open into a fresh subdir must Just Work: ensure-parent-dir!
  ;; creates the (possibly nested) parent before the lock's .lock_status sidecar
  ;; — the first filesystem touch — would otherwise fail on the missing dir.
  (let [base (doto (java.io.File/createTempFile "dj-recorder-parentdir" "")
               (.delete))                          ; reuse the name as a dir root
        nested (io/file base "a" "b" "state.edn")
        path   (.getPath nested)]
    (.deleteOnExit base)
    (try
      (is (not (.exists (.getParentFile nested))) "parent dir absent to start")
      (s/ensure-parent-dir! path)
      (is (.isDirectory (.getParentFile nested)) "parent dir (and ancestors) created")
      ;; and the writer now opens into it and round-trips
      (with-open [w (s/open-writer path)]
        (proto/append! w {:k 1}))
      (is (= {:k 1} (:state (s/read-log {} path))))
      ;; a bare filename (no parent) is a harmless no-op, not a throw
      (is (nil? (s/ensure-parent-dir! "state.edn")))
      (finally
        ;; best-effort recursive cleanup of the temp tree
        (doseq [f (reverse (file-seq base))] (.delete ^java.io.File f))))))

(deftest append-refuses-non-edn-patch
  ;; The write-path choke point: a patch carrying a live object (here a fn) is
  ;; outside patch's replayable-EDN whitelist, so the structural gate
  ;; (`patch/assert-edn!`, [D4]) refuses it at append! — pointing at the
  ;; offending leaf's path — and the log is left byte-for-byte untouched rather
  ;; than persisting an unreadable line for replay to choke on later.
  (let [path (tmp-path)]
    (with-open [w (s/open-writer path)]
      (proto/append! w {:ok 1})                        ; one good, committed record
      (let [before (file-bytes path)
            ex     (try (proto/append! w {:bad (fn [] 42)}) nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (some? ex) "a non-EDN patch is refused at append!")
        (is (= [:bad] (:dj.recorder/edn-path (ex-data ex)))
            "the structural gate locates the offending leaf by path")
        (is (contains? (ex-data ex) :value)
            "and carries the offending value for attribution")
        (is (= before (file-bytes path))
            "the refused append left the log untouched")))
    ;; and the good record alone replays cleanly — the reject wrote nothing.
    (is (= {:ok 1} (:state (s/read-log {} path))))))

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
      (proto/append! w {:a 1})
      (proto/append! w {:b 2}))
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
      (proto/append! w {:a 1}))
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
      (proto/append! w {:a 1}))
    (spit path "{:partial" :append true)
    (s/truncate-to path (:offset (:torn-tail (s/read-log {} path))))
    (let [res (s/read-log {} path)]
      (is (nil? (:torn-tail res)) "torn tail gone after truncate")
      (is (= {:a 1} (:state res)) "intact prefix preserved")
      (is (= 1 (:count res))))))

(deftest truncate-rejects-out-of-bounds-offset
  ;; NIO's truncate silently no-ops on an oversized offset; we guard against a
  ;; stale :offset (from a read of a file that has since shrunk) masking a bug.
  (let [path (tmp-path)]
    (with-open [w (s/open-writer path)]
      (proto/append! w {:a 1}))
    (let [size (file-bytes path)]
      (is (thrown? clojure.lang.ExceptionInfo (s/truncate-to path (inc size)))
          "an offset past the end is refused")
      (is (thrown? clojure.lang.ExceptionInfo (s/truncate-to path -1))
          "a negative offset is refused")
      (is (= size (file-bytes path)) "a refused truncate left the file untouched")
      (is (nil? (s/truncate-to path size)) "an in-bounds offset (= size) is a valid no-op"))))

(deftest append-ignores-ambient-print-length
  ;; a caller's *print-length* must never truncate a record into `...`.
  (let [path (tmp-path)
        big  {:xs (vec (range 50))}]
    (binding [*print-length* 3
              *print-level*  1]
      (with-open [w (s/open-writer path)]
        (proto/append! w big)))
    (is (= big (:state (s/read-log {} path)))
        "the full structure persists despite ambient print limits")))

(deftest file-lock-mechanics
  (let [path (tmp-path)
        lock (s/file-lock path)]
    (.lock ^Lock lock)
    (is (.exists (io/file (str path ".lock"))) ".lock marker written")
    (is (.exists (io/file (str path ".lock_status"))) ".lock_status sidecar created")
    (testing "a second lock in the same JVM fails fast with a distinct error"
      (let [lock2 (s/file-lock path)
            ex    (try (.lock ^Lock lock2) nil
                       (catch clojure.lang.ExceptionInfo e e))]
        (is (some? ex) "the overlapping lock throws (does not block or return)")
        (is (= :this-process (:dj.recorder/locked-by (ex-data ex)))
            "and names the already-open-in-this-process case, not a bare NIO exception")
        (is (false? (.tryLock ^Lock lock2)) "tryLock reports the contention as false")
        (is (false? (.tryLock ^Lock lock2 10 TimeUnit/MILLISECONDS))
            "the timed tryLock polls to its deadline, then gives up with false")))
    (.unlock ^Lock lock)
    (is (not (.exists (io/file (str path ".lock"))))
        ".lock marker removed on unlock")
    (is (.exists (io/file (str path ".lock_status")))
        ".lock_status kept on unlock (split-brain safety)")
    (testing "unlock is idempotent"
      (is (nil? (.unlock ^Lock lock)) "a second unlock no-ops rather than throwing"))
    (testing "the lock is re-acquirable once released"
      (is (true? (.tryLock ^Lock lock)) "a fresh tryLock succeeds after unlock")
      (.unlock ^Lock lock))
    (io/delete-file (str path ".lock_status") true)))
