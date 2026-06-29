(ns dj.recorder.storage
  "The durable substrate for dj.recorder: an append-only EDN log, a
  torn-tail-aware rehydrate, and a single-writer file lock.

  Pure storage — no dispatch, no public API; those sit on top (items 3-4).
  Design: agent/ledger/2026-06-27-runtime-design-deep-dive.md §4 (crash
  safety without fsync), §6 (rehydration / locking).

  Crash-safety contract (§4): append-only + flush guarantees that
  *already-committed* records can never be corrupted by a later failed
  write — only the trailing record can be torn. We do NOT silently drop a
  torn tail; `read-log` surfaces it (offset + raw bytes) for inspection,
  and the caller decides whether to discard it (item 4). No fsync in alpha:
  flush reaches the OS page cache, not necessarily the platter.

  Log format: one patch per line, `(pr-str patch)` + \\n, UTF-8. pr-str
  escapes embedded newlines in strings, so a record is always exactly one
  physical line and the \\n is a reliable record delimiter. The marker
  records (patch ns) print as their tagged literals and re-read via
  `patch/data-readers`."
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [dj.recorder.patch :as patch])
  (:import [java.io ByteArrayInputStream PushbackReader Writer]
           [java.nio.charset StandardCharsets]
           [java.nio.channels FileChannel FileLock OverlappingFileLockException]
           [java.nio.file Files Paths StandardOpenOption]
           [java.util Arrays]
           [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Append-only writer
;; ---------------------------------------------------------------------------

(defprotocol AppendLog
  (append! [log patch]
    "Append one patch to the log as a single EDN line and flush. The flush
    pushes to the OS page cache (not necessarily the platter — see §4)."))

(defn open-writer
  "Open `path` for append (creating it if absent) and return an `AppendLog`
  that is also `java.lang.AutoCloseable`. Single-writer: hold the file lock
  (`file-lock`) around this for the life of the writer."
  ^java.lang.AutoCloseable [path]
  (let [w ^Writer (io/writer path :append true :encoding "UTF-8")]
    (reify
      AppendLog
      (append! [_ patch]
        ;; Bind print-length/level to nil so a caller's ambient *print-length*
        ;; can never truncate a large structure into an unreadable `...` —
        ;; that would silently corrupt the log. Default print (not print-dup)
        ;; so markers emit their tagged literals.
        (binding [*print-length* nil
                  *print-level*  nil]
          (.write w (pr-str patch))
          (.write w "\n")
          (.flush w)))
      java.lang.AutoCloseable
      (close [_] (.close w)))))

;; ---------------------------------------------------------------------------
;; Torn-tail-aware rehydrate
;; ---------------------------------------------------------------------------

(defn- last-newline-index
  "Index of the last \\n byte in `ba`, or -1 if none."
  ^long [^bytes ba]
  (loop [i (dec (alength ba))]
    (cond
      (neg? i)            -1
      (== 10 (aget ba i)) i
      :else               (recur (dec i)))))

(defn- replay-bytes
  "Fold the complete-record region `good` (a byte-array of whole, \\n-terminated
  EDN lines) onto `baseline` with `apply-patch`. Returns [state count]. A line
  that fails to parse here is genuine mid-log corruption (not the expected torn
  tail) and is left to throw loudly."
  [baseline ^bytes good]
  (let [sentinel (Object.)
        opts     {:eof sentinel
                  :readers patch/data-readers
                  :default tagged-literal}]
    (with-open [rdr (PushbackReader.
                     (io/reader (ByteArrayInputStream. good) :encoding "UTF-8"))]
      (loop [state baseline n 0]
        (let [form (edn/read opts rdr)]
          (if (identical? sentinel form)
            [state n]
            (recur (patch/apply-patch state form) (inc n))))))))

(defn read-log
  "Replay the append-only log at `path`, folding patches onto `baseline`.

  Returns:
    {:state     <rehydrated state>
     :count     <number of complete records replayed>
     :torn-tail nil
              | {:offset <byte index where the partial trailing record starts>
                 :bytes  <byte-array of the raw partial — the source of truth>
                 :text   <the partial decoded as UTF-8, for human inspection>}}

  `:bytes` is the byte-exact fragment; `:text` is a convenience decode for
  reading it. The decode is lossy at the tear: if the crash split a multi-byte
  character, the dangling bytes can't form a code point and collapse to a single
  U+FFFD (the `String(byte[], UTF-8)` REPLACE behavior). For byte-level forensics
  on such a tail, use `:bytes`, not `:text`.

  A missing or empty file yields `baseline` with no torn tail. A trailing
  record without its terminating \\n is the in-flight tx lost to a crash
  (§4): everything before the last \\n is intact and replayed; the partial
  is surfaced, never silently dropped. The caller picks the policy (keep =
  surface/raise, or truncate to `:offset` to discard)."
  [baseline path]
  (let [f (io/file path)]
    (if-not (.exists f)
      {:state baseline :count 0 :torn-tail nil}
      (let [ba    ^bytes (Files/readAllBytes (.toPath f))
            len   (alength ba)
            start (inc (last-newline-index ba))   ; 0 when no \n at all
            good  (Arrays/copyOfRange ba 0 (int start))
            torn  (Arrays/copyOfRange ba (int start) len)
            [state n] (replay-bytes baseline good)]
        {:state state
         :count n
         :torn-tail (when (pos? (alength torn))
                      {:offset start
                       :bytes  torn
                       :text   (String. torn StandardCharsets/UTF_8)})}))))

(defn truncate-to
  "Truncate the log file at `path` to `offset` bytes — the `:discard` policy
  for a torn tail (§4). `offset` is the `:offset` from `read-log`'s
  `:torn-tail`, i.e. just past the last good record's \\n, so this drops the
  partial and leaves the intact prefix."
  [path offset]
  (with-open [ch (FileChannel/open (.toPath (io/file path))
                                   (into-array StandardOpenOption
                                               [StandardOpenOption/WRITE]))]
    (.truncate ch (long offset))
    nil))

;; ---------------------------------------------------------------------------
;; Single-writer file lock (ported from durable2; the split-brain reasoning
;; below is its, and worth keeping)
;; ---------------------------------------------------------------------------

(defn file-lock
  "A `java.util.concurrent.locks.Lock` backed by a `FileChannel` lock on a
  `<path>.lock_status` sidecar — prevents two processes from opening the same
  log. `lock` also writes a human-visible `<path>.lock` marker; `unlock`
  releases the channel lock and removes the marker but NEVER deletes
  `.lock_status`.

  Why keep `.lock_status` forever: POSIX file locks are on the inode, not the
  path. If we deleted the lock file on unlock, a process already blocked on a
  FileChannel to that inode could be joined by a third process that sees no
  lock file, creates a fresh one, and locks it — both then believe they have
  exclusive access (split brain). Never deleting the sidecar avoids that. (The
  visible `.lock` marker is for outside awareness only; coordination is the
  `.lock_status` channel lock.)"
  [path]
  (let [path-str    (str path)
        status-path (Paths/get (str path-str ".lock_status") (into-array String []))
        lock-path   (Paths/get (str path-str ".lock") (into-array String []))
        channel     (FileChannel/open status-path
                                      (into-array StandardOpenOption
                                                  [StandardOpenOption/CREATE
                                                   StandardOpenOption/WRITE]))
        a-lock      (atom nil)]
    (reify Lock
      (lock [_]
        (reset! a-lock (.lock channel))
        (spit (str path-str ".lock") "locked"))
      (unlock [_]
        ;; Single-threaded lifecycle (lock once, unlock once); no internal
        ;; guard against double-unlock — wrap if that's needed.
        (when-let [^FileLock l @a-lock]
          (.release l)
          (.close channel)
          (Files/deleteIfExists lock-path))))))
