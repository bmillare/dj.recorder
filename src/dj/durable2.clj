(ns dj.durable2
  "A durable, append-only key-value storage system supporting EDN and transactions."
  (:require [dj.durable.protocols :as p]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(set! *warn-on-reflection* true)

(defn edn-file-storage
  "Creates an EDN-based append-only file storage backend."
  [path]
  (let [writer ^java.io.Writer (io/writer path :append true)]
    (reify
      p/AppendableStorage
      (append! [_ obj]
        ;; edn is self delimiting, so don't need to put any other info here
        ;; but putting \n just for readability when debugging
        (.write writer (str (pr-str obj) "\n"))
        (.flush writer))
      java.lang.AutoCloseable
      (close [_]
        (.close writer)))))

(defn read-edn-diff-log-file
  "Reads an EDN diff log file from disk and replays it to construct the current state."
  [path & [{:keys [ignore-missing]
            :or {ignore-missing true}}]]
  (try
    (with-open [reader (-> path
                           io/reader
                           (java.io.PushbackReader.))]
      (let [sentinel (Object.)]
        (loop [state (transient {})]
          (let [diff (edn/read {:eof sentinel
                                :default tagged-literal}
                               reader)]
            (if (= sentinel diff)
              (persistent! state)
              (recur (reduce-kv (fn [state' k v]
                                  (if (nil? v)
                                    (dissoc! state' k)
                                    (assoc! state' k v)))
                                state
                                diff)))))))
    (catch java.io.FileNotFoundException e
      (if ignore-missing
        {}
        (throw e)))))

(deftype AppendDiffDb [_agent storage ^java.util.concurrent.locks.Lock db-lock a-closed? ^java.util.concurrent.locks.Lock mutex]
  p/DiffTransactor
  (diff-tx! [db diff-fn]
    (when-not @a-closed?
      ;; - close flag is on when we intend to close, not when functionally closed
      ;; - once atomic intention to close, we stop taking txns
      ;; - deref still works but db becomes immutable
      (let [new-state-promise (promise)]
        (send-off _agent
                  ;; Currently we DON'T handle agent crashes due to
                  ;; thrown exceptions from +writer/diff-fn, need to
                  ;; think about how we want to do this in later
                  ;; iterations of this code, maybe we just reject a
                  ;; throw exception but we need a way to get this
                  ;; exception to the caller, maybe via an error queue
                  ;; or something, maybe return a request id and create
                  ;; an exception map, or maybe create a promise of the
                  ;; new state/exception. Main problem with promise is
                  ;; it is possible it can be delivered before the agent
                  ;; state is updated, although if we consider the
                  ;; storage to be ground truth and deref just lags,
                  ;; then this is also fine, you won't ever be normally
                  ;; expected to coordinate with other threads based on
                  ;; what is in the agent
                  (fn +writer [state]
                    (try
                      (let [ ;; we delegate diff minimization to tx-helpers
                            diff (diff-fn state)
                            new-state (reduce-kv (fn [state' k v]
                                                   (if (nil? v)
                                                     (dissoc state' k)
                                                     (assoc state' k v)))
                                                 state
                                                 diff)]
                        ;; ignore no change
                        (if (= new-state state)
                          (do
                            (deliver new-state-promise state)
                            state)
                          (do
                            (p/append! storage diff)
                            (deliver new-state-promise new-state)
                            new-state)))
                      (catch Exception e
                        (deliver new-state-promise e)
                        state)))))))
  clojure.lang.IDeref
  (deref [_]
    @_agent)
  p/Closeable
  (close! [_]
    (.lock mutex)
    (when-not @a-closed?
      (reset! a-closed? true)
      (.unlock db-lock)
      (.close ^java.lang.AutoCloseable storage))
    (.unlock mutex))
  (closed? [_]
    @a-closed?)
  p/Compactable
  (compact! [_]
    ;; TODO later if really needed
    ))

(extend-protocol clojure.core.protocols/Datafiable
  AppendDiffDb
  (datafy [this]
    (with-meta
      {:storage-type (type (.storage this))
       :closed? (p/closed? this)}
      {:clojure.datafy/obj this})))

(defn db-file-lock
  "Acquires a file-based lock for the given database path to ensure single-process access."
  [db-path-str]
  (let [status-path (java.nio.file.Paths/get (str db-path-str ".lock_status") (into-array String []))
        lock-path (java.nio.file.Paths/get (str db-path-str ".lock") (into-array String []))
        channel (java.nio.channels.FileChannel/open status-path (into-array [java.nio.file.StandardOpenOption/CREATE 
                                                                             java.nio.file.StandardOpenOption/WRITE]))
        a-lock (atom nil)]
    (reify
      java.util.concurrent.locks.Lock
      (lock [_]
        (reset! a-lock (.lock channel))
        (spit (str db-path-str ".lock") "locked"))
      (unlock [_] (when @a-lock ;; not worrying about race condition
                    ;; here, this should only be run after
                    ;; a valid lock is created and we will
                    ;; never undo the lock multiple times
                    ;; - use wrapper to make thread safe
                    (.release ^java.nio.channels.FileLock @a-lock)
                    (.close channel)
                    (java.nio.file.Files/deleteIfExists lock-path)
                    #_ (java.nio.file.Files/deleteIfExists status-path)  ;; Leaving this in would make the filesystem cleaner, but would create split-brain vulnerability
                    ;; In a POSIX system, File locks hold the lock on the inode, not the file path string. If Process A has the lock file and eventually deletes it upon unlocking, Process B might have already opened a FileChannel to that exact inode but is waiting for the lock. Process C then starts up, sees no .lock file, creates a new file, and locks it. Now both Process B and Process C believe they have exclusive access. Fix: Never delete the .lock file. Simply unlock and close the channel. Renamed .lock file to .lock_status
                    ;; BM: one problem we still have though is from the outside, we can't tell which files are locked by the db
                    ;; - we can state that we don't assume this is a coordination mechanism, but just a prevent stupidity situation of accidently trying to modify the same db
                    ;; - ultimate fix is to have a .lock file that is just for outsider awareness, but actual coordination is via .lock_status
                    )))))

(defn edn-diff-db
  "Initializes an EDN-backed differential database instance at the given path."
  [path]
  (let [db-lock (db-file-lock path)]
    (.lock ^java.util.concurrent.locks.Lock db-lock)
    (AppendDiffDb. (agent (read-edn-diff-log-file path))
                   (edn-file-storage path)
                   db-lock
                   (atom false)
                   (java.util.concurrent.locks.ReentrantLock.))))

(defn diff-tx!
  "Applies a state-transforming function `f` to the database as a transaction."
  ([db f]
   (p/diff-tx! db f))
  ([db f & args]
   (p/diff-tx! db (fn [state]
                    (apply f state args)))))

(defn close!
  "Closes the database, preventing further transactions and releasing resources."
  [db]
  (p/close! db))

(defn closed?
  "Returns true if the database is closed."
  [db]
  (p/closed? db))

;; ----------------------------------------------------------------------

(defn d-assoc
  "Computes a diff for associating key-value pairs into map `m`.
  Can also take deletes (by passing nil values)."
  ([m k v]
   (if (= (m k) v)
     {}
     {k v}))
  ([m k v & kvs]
   (let [diff1 (d-assoc m k v)]
     (reduce (fn [ret [k v]]
               (if (= (m k) v)
                 ret
                 (assoc ret
                        k v)))
             diff1
             (partition 2 kvs)))))

(defn db-assoc
  "Applies an associative transaction to the database, persisting the diff."
  [db & kvs]
  (p/diff-tx! db (fn [state]
                   (apply d-assoc state kvs))))

(defn d-dissoc
  "Computes a diff that removes the specified keys `ks` from map `m`."
  [m & ks]
  (reduce (fn [ret k]
            (if (contains? m k)
              (assoc ret k nil)
              ret))
          {}
          ks))

(defn db-dissoc
  "Applies a dissociation transaction to the database, persisting the diff."
  [db & ks]
  (p/diff-tx! db (fn [state]
                   (apply d-dissoc state ks))))

(defn d-update
  "Computes a diff for updating a key `k` in map `m` using function `f`."
  ([m k f]
   (d-assoc m k (f (m k))))
  ([m k f default-v]
   (d-assoc m k (f (get m k default-v)))))

(defn d-merge
  "Computes a diff for merging one or more maps into `m`."
  ([m m1]
   (reduce-kv (fn [ret k v]
                (merge ret
                       (d-assoc m k v)))
              {}
              m1))
  ([m m1 & ms]
   (let [merged-changes (apply merge m1 ms)]
     (d-merge m merged-changes))))

(defn db-merge
  "Applies a merge transaction to the database, persisting the diff."
  [db & ms]
  (p/diff-tx! db (fn [state]
                   (apply d-merge state ms))))

(comment
  (d-merge {:a 1
            :b 3
            :c 4}
           {:a 2
            :b 3
            :d nil}
           {:c nil})
  (d-dissoc {:a 1}
            :c
            :b
            :a)
  (d-update {:a 1}
            :b inc 0)

  )
