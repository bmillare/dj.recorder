(ns dj.recorder-test
  "Exercises the public API / lifecycle (alpha item 4): open/record!/@db/await/
  close!, the literal-patch arity, record!-sync, read-your-writes, durable
  round-trip across a close+re-open, the torn-tail :surface/:discard policy, the
  single-writer lock, and the closed-rejects-writes contract. Runtime deep-dive
  §1/§3/§4/§5/§6."
  (:refer-clojure :exclude [await])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [dj.recorder :as r]
            [dj.recorder.patch :as p])
  (:import [java.nio.file Files Paths StandardOpenOption]))

;; ---------------------------------------------------------------------------
;; Temp-file plumbing: a fresh, nonexistent log path per test, with all of the
;; sidecars (.lock_status / .lock) cleaned up afterwards.
;; ---------------------------------------------------------------------------

(defn- fresh-path []
  (let [f (java.io.File/createTempFile "djrec-" ".edn")]
    (.delete f)                               ; want the path, not the (empty) file
    (.getAbsolutePath f)))

(defn- cleanup [path]
  (doseq [suffix ["" ".lock" ".lock_status"]]
    (.delete (io/file (str path suffix)))))

(defmacro with-path [[sym] & body]
  `(let [~sym (fresh-path)]
     (try ~@body (finally (cleanup ~sym)))))

(defn- append-raw!
  "Append raw bytes to the log file out-of-band (to fabricate a torn tail)."
  [path ^String s]
  (Files/write (Paths/get path (into-array String []))
               (.getBytes s "UTF-8")
               (into-array StandardOpenOption
                           [StandardOpenOption/CREATE StandardOpenOption/APPEND])))

;; ---------------------------------------------------------------------------
;; open / record! / @db
;; ---------------------------------------------------------------------------

(deftest fresh-open-and-record
  (with-path [path]
    (let [db (r/open path)]
      (try
        (is (= {} @db) "a fresh db derefs to the default baseline")
        (let [p (r/record! db (fn [_] {:user {:name "Bob"}}))]
          (is (= {:user {:name "Bob"}} @p) "the promise resolves to the new realized state")
          (is (= {:user {:name "Bob"}} @db) "after the promise resolves, @db reflects the write"))
        ;; authoring fn reads prior state -> race-free read-modify-write
        (r/record!-sync db (fn [s] {:user {:age (if (:user s) 31 0)}}))
        (is (= {:user {:name "Bob" :age 31}} @db))
        (finally (r/close! db))))))

(deftest custom-baseline
  (with-path [path]
    (let [db (r/open path {:baseline []})]
      (try
        (is (= [] @db) "root state can be a non-map baseline")
        (is (= [1 2 3] (r/record!-sync db [1 2 3])) "vector-patch appends")
        (finally (r/close! db))))))

(deftest literal-patch-arity
  (with-path [path]
    (let [db (r/open path)]
      (try
        (is (= {:k 1} (r/record!-sync db {:k 1})) "a non-fn arg is treated as a literal patch")
        (is (= {:k 1 :j 2} (r/record!-sync db {:j 2})))
        (finally (r/close! db))))))

(deftest record-sync-rethrows-on-bad-patch
  (with-path [path]
    (let [db (r/open path {:baseline {:items 5}})]            ; :items is a scalar
      (try
        ;; #dj.recorder/splice requires a vector -> apply-patch throws -> rejected tx
        (is (thrown? Throwable
                     (r/record!-sync db {:items (p/read-splice [{:at 0 :+ [9]}])}))
            "record!-sync rethrows the rejected tx's Throwable")
        (is (nil? (r/halted db)) "a patch error does NOT halt the db")
        (is (= {:items 5 :ok true} (r/record!-sync db {:ok true}))
            "and the db keeps working after a rejected tx")
        (finally (r/close! db))))))

(deftest read-your-writes-via-await
  (with-path [path]
    (let [db (r/open path)]
      (try
        (dotimes [i 50] (r/record! db {(keyword (str "k" i)) i}))  ; fire-and-forget
        (r/await db)                                               ; barrier
        (is (= 50 (count @db)) "await drains all prior fire-and-forget writes")
        (finally (r/close! db))))))

;; ---------------------------------------------------------------------------
;; Durable round-trip across a restart
;; ---------------------------------------------------------------------------

(deftest survives-close-and-reopen
  (with-path [path]
    (let [db (r/open path)]
      (r/record!-sync db {:user {:name "Bob"}})
      (r/record!-sync db (fn [_] {:user {:age 31}}))
      (r/record!-sync db {:items [1 2 3]})
      (r/close! db))
    ;; a brand-new handle replays the on-disk log onto baseline -> same state
    (let [db2 (r/open path)]
      (try
        (is (= {:user {:name "Bob" :age 31} :items [1 2 3]} @db2)
            "re-open rehydrates the full state from the durable log")
        (finally (r/close! db2))))))

(deftest close-drains-queued-work
  (with-path [path]
    (let [db (r/open path)]
      (dotimes [i 100] (r/record! db {(keyword (str "k" i)) i}))  ; don't await
      (r/close! db))                                              ; close must drain first
    (let [db2 (r/open path)]
      (try
        (is (= 100 (count @db2)) "close! drained all in-flight work before releasing")
        (finally (r/close! db2))))))

(deftest close-is-idempotent-and-rejects-writes
  (with-path [path]
    (let [db (r/open path)]
      (r/record!-sync db {:a 1})
      (r/close! db)
      (r/close! db)                                       ; second close no-ops
      (is (= {:a 1} @db) "deref still works after close (immutable view)")
      (is (thrown? Throwable (r/record! db {:b 2})) "writes throw after close"))))

;; ---------------------------------------------------------------------------
;; Torn-tail policy (§4)
;; ---------------------------------------------------------------------------

(deftest torn-tail-surface-raises-and-frees-lock
  (with-path [path]
    ;; one good record, then a partial trailing record (no terminating \n)
    (let [db (r/open path)]
      (r/record!-sync db {:good 1})
      (r/close! db))
    (append-raw! path "{:torn ")                          ; fabricate a crash tail
    (let [ex (try (r/open path) nil (catch clojure.lang.ExceptionInfo e e))]
      (is (some? ex) "default :surface raises on a torn tail")
      (let [tt (:dj.recorder/torn-tail (ex-data ex))]
        (is (= "{:torn " (:text tt)) "the ex carries the raw partial for inspection")
        (is (number? (:offset tt)) "and its byte offset")))
    ;; the lock was released on the failed open -> we can open again (here: discard)
    (let [db (r/open path {:on-torn-tail :discard})]
      (try
        (is (= {:good 1} @db) "discard drops the partial and keeps the intact prefix")
        (finally (r/close! db))))))

(deftest torn-tail-discard-then-writable
  (with-path [path]
    (let [db (r/open path)]
      (r/record!-sync db {:good 1})
      (r/close! db))
    (append-raw! path "{:torn ")
    (let [db (r/open path {:on-torn-tail :discard})]
      (try
        (is (= {:good 1} @db))
        (is (= {:good 1 :more 2} (r/record!-sync db {:more 2})) "can record after a discard")
        (finally (r/close! db)))
      ;; and the discard+new write round-trips cleanly on the next open
      (let [db2 (r/open path)]
        (try (is (= {:good 1 :more 2} @db2)) (finally (r/close! db2)))))))

;; ---------------------------------------------------------------------------
;; Single-writer lock (§6)
;; ---------------------------------------------------------------------------

(deftest single-writer-lock-excludes-second-open
  (with-path [path]
    (let [db (r/open path)]
      (try
        (is (thrown? Exception (r/open path))
            "a second open on the same log is refused by the file lock")
        (finally (r/close! db)))
      ;; once closed (lock released), a fresh open succeeds
      (let [db2 (r/open path)]
        (try (is (some? db2)) (finally (r/close! db2)))))))
