(ns dj.recorder.storage
  "The durable substrate for dj.recorder: an append-only EDN log, a
  torn-tail-aware rehydrate, and a single-writer file lock.

  Pure storage — no dispatch, no public API; those sit on top (items 3-4).
  Design: agent/ledger/2026-06-27-runtime-design-deep-dive.md §4 (crash
  safety without fsync), §6 (rehydration / locking).

  Crash-safety contract (§4), scoped precisely: append-only + flush protects
  *already-committed* records against a PROCESS crash — flushed bytes live in
  the OS page cache and survive the JVM dying; only the trailing record can
  be torn. It does NOT protect against power loss or a kernel panic: without
  fsync the OS may write dirty pages back out of order, so such a crash can
  leave a hole mid-log even though later bytes survived. That failure mode
  presents as a parse error during replay, not a torn tail. Accepted for
  alpha; the natural hardening later is an opt-in fsync mode
  (`FileChannel/force`, per append or per N) plus a per-record checksum so
  mid-log corruption is detected and located rather than inferred.

  We do NOT silently drop a torn tail; `read-log` surfaces it (offset + raw
  bytes) for inspection, and the caller decides whether to discard it
  (item 4).

  Log format: one patch per line, `(pr-str patch)` + \\n, UTF-8. With
  `*print-readably*` pinned true (see `append!`), pr-str escapes embedded
  newlines in strings, so a record is always exactly one physical line and
  the \\n is a reliable record delimiter. Every record passes two gates
  *before* it touches the file: `patch/assert-edn!` (the structural whitelist —
  records-used-as-patch, live objects, functions are rejected with a path to
  the offending leaf, [D4]), then a serialize-and-reparse round-trip through
  our own EDN reader as a backstop. A non-EDN value (a function, an atom, a
  `#object[...]`) is refused at `append!` instead of being written and misread
  later. The marker records (patch ns) print as their tagged literals and
  re-read via `patch/data-readers`."
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [dj.recorder.patch :as patch])
  (:import [java.io ByteArrayInputStream PushbackReader Writer]
           [java.nio.charset StandardCharsets]
           [java.nio.channels FileChannel FileLock OverlappingFileLockException]
           [java.nio.file Files Paths StandardOpenOption]
           [java.util Arrays]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Append-only writer
;; ---------------------------------------------------------------------------

(defprotocol AppendLog
  (append! [log patch]
    "Append one patch to the log as a single EDN line and flush. The flush
    pushes to the OS page cache — durable across a process crash, not
    necessarily across power loss (see the ns docstring). Throws (leaving
    the log untouched) if `patch` does not round-trip as EDN."))

(defn- assert-round-trips!
  "Verify that `line` (the serialized form of `patch`) parses back through the
  same reader configuration `read-log` will use — except that unknown tags
  THROW here instead of falling back to `tagged-literal`.

  Why the asymmetry: on the read side, `:default tagged-literal` is the right
  forward-compatibility posture (an old binary can still replay a log written
  by a newer one with new marker tags). On the *write* side, current code
  emitting a tag its own readers don't know is almost always a bug — most
  commonly a live object printed as `#object[...]`, which `tagged-literal`
  would happily launder into an opaque TaggedLiteral that corrupts state only
  far downstream. Refusing at the single write-path choke point turns that
  into an immediate, attributable error on the per-tx promise.

  Cost: one in-memory parse per append — negligible next to the flush that
  follows."
  [line patch]
  (try
    (edn/read-string {:readers patch/data-readers
                      :default (fn [tag _value]
                                 (throw (ex-info (str "unknown tag #" tag)
                                                 {:tag tag})))}
                     line)
    (catch Throwable e
      (throw (ex-info (str "dj.recorder: patch does not round-trip as EDN — refusing to"
                           " append it (the log is untouched). Patches must be plain EDN"
                           " data (or the patch ns's markers); a function, atom, or other"
                           " live object cannot be persisted. See ex-data's"
                           " :dj.recorder/invalid-patch for the value and :line for its"
                           " serialized form.")
                      {:dj.recorder/invalid-patch patch
                       :line line}
                      e)))))

(defn open-writer
  "Open `path` for append (creating it if absent) and return an `AppendLog`
  that is also `java.lang.AutoCloseable`. Single-writer: hold the file lock
  (`file-lock`) around this for the life of the writer."
  ^java.lang.AutoCloseable [path]
  (let [w ^Writer (io/writer path :append true :encoding "UTF-8")]
    (reify
      AppendLog
      (append! [_ patch]
        ;; Structural EDN gate ([D4]): reject records-used-as-patch, live
        ;; objects, functions — anything outside patch's replayable-EDN
        ;; whitelist — BEFORE serializing, with a `:dj.recorder/edn-path` to
        ;; the offending leaf. Throws (log untouched) on failure; the
        ;; serialize-and-reparse `assert-round-trips!` below is the remaining
        ;; backstop against a print-method/reader bug on an otherwise-whitelisted
        ;; value.
        (patch/assert-edn! patch)
        ;; Pin EVERY print var the one-line/readable invariant depends on —
        ;; not just the truncating ones. A caller's ambient *print-length*/
        ;; *print-level* could truncate a large structure into `...`; a falsey
        ;; *print-readably* would emit raw (unescaped) newlines and tear the
        ;; line-per-record framing; *print-dup*/*print-meta* change the output
        ;; format into something edn/read rejects. Whether such bindings even
        ;; convey onto the drainer thread is a dispatch implementation detail
        ;; this namespace must not depend on. *print-namespace-maps* is pinned
        ;; off purely so the on-disk form is canonical.
        (let [^String line (binding [*print-length*         nil
                                     *print-level*          nil
                                     *print-readably*       true
                                     *print-dup*            false
                                     *print-meta*           false
                                     *print-namespace-maps* false]
                             (pr-str patch))]
          ;; Validate BEFORE writing: a rejected patch leaves the log intact.
          (assert-round-trips! line patch)
          (.write w line)
          (.write w "\n")
          (.flush w)))
      java.lang.AutoCloseable
      (close [_] (.close w)))))

;; ---------------------------------------------------------------------------
;; Torn-tail-aware rehydrate
;; ---------------------------------------------------------------------------

(defn- last-newline-index
  "Index of the last \\n byte in `ba`, or -1 if none. Safe on raw UTF-8: a
  continuation byte can never collide with 0x0A."
  ^long [^bytes ba]
  (loop [i (dec (alength ba))]
    (cond
      (neg? i)            -1
      (== 10 (aget ba i)) i
      :else               (recur (dec i)))))

(defn- replay-bytes
  "Fold the complete-record region — the first `len` bytes of `ba`, i.e. whole
  \\n-terminated EDN lines — onto `baseline` with `apply-patch`. Reads the
  region in place (no copy). Returns [state count]. A line that fails to parse
  here is genuine mid-log corruption (not the expected torn tail) and is left
  to throw loudly."
  [baseline ^bytes ba ^long len]
  (let [sentinel (Object.)
        opts     {:eof sentinel
                  :readers patch/data-readers
                  :default tagged-literal}]
    (with-open [rdr (PushbackReader.
                     (io/reader (ByteArrayInputStream. ba 0 len)
                                :encoding "UTF-8"))]
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
  surface/raise, or truncate to `:offset` to discard).

  Memory: the whole file is read into one byte array and replayed in place
  (only the torn fragment, if any, is copied — it escapes in the return
  value). That caps replayable log size at available heap; acceptable for
  alpha, and the place a streaming replay would slot in later."
  [baseline path]
  (let [f (io/file path)]
    (if-not (.exists f)
      {:state baseline :count 0 :torn-tail nil}
      (let [ba    ^bytes (Files/readAllBytes (.toPath f))
            len   (alength ba)
            start (inc (last-newline-index ba))   ; 0 when no \n at all
            [state n] (replay-bytes baseline ba start)]
        {:state state
         :count n
         :torn-tail (when (< start len)
                      (let [torn (Arrays/copyOfRange ba (int start) len)]
                        {:offset start
                         :bytes  torn
                         :text   (String. torn StandardCharsets/UTF_8)}))}))))

(defn truncate-to
  "Truncate the log file at `path` to `offset` bytes — the `:discard` policy
  for a torn tail (§4). `offset` is the `:offset` from `read-log`'s
  `:torn-tail`, i.e. just past the last good record's \\n, so this drops the
  partial and leaves the intact prefix.

  Throws if `offset` is outside `[0, current size]` — NIO's `truncate`
  silently no-ops on an oversized offset, which would mask a caller bug (a
  stale offset from a previous read of a file that has since shrunk)."
  [path offset]
  (with-open [ch (FileChannel/open (.toPath (io/file path))
                                   (into-array StandardOpenOption
                                               [StandardOpenOption/WRITE]))]
    (let [size (.size ch)]
      (when-not (<= 0 (long offset) size)
        (throw (ex-info (str "dj.recorder: truncate offset " offset
                             " is outside the file's bounds [0, " size "] — refusing"
                             " (a stale :offset from an earlier read-log?)")
                        {:path (str path) :offset offset :size size})))
      (.truncate ch (long offset)))
    nil))

;; ---------------------------------------------------------------------------
;; Single-writer file lock (evolved from durable2; the split-brain reasoning
;; below is its, and worth keeping)
;; ---------------------------------------------------------------------------

(defn file-lock
  "A `java.util.concurrent.locks.Lock` backed by a `FileChannel` lock on a
  `<path>.lock_status` sidecar — prevents two processes from opening the same
  log. `lock` also writes a human-visible `<path>.lock` marker (best-effort:
  a failure to write the advisory marker never strands the real lock);
  `unlock` releases the channel lock, removes the marker, and NEVER deletes
  `.lock_status`.

  FAIL-FAST, deviating from `Lock`'s blocking contract on purpose: `lock`
  either acquires immediately or throws an ex-info saying who has it — the
  database-open semantic (\"file is locked by another process\") rather than
  a silent indefinite hang. The two contention cases get distinct errors:
  held by another process, or already open in THIS process (the common REPL/
  forgot-to-close! mistake, which raw NIO reports as a bare
  OverlappingFileLockException). Use `tryLock` for a boolean attempt, or the
  timed `tryLock` to poll up to a deadline. `newCondition` is unsupported.

  Lifecycle: the sidecar channel is opened lazily inside acquisition, so
  merely constructing this object — or a failed acquisition — leaks nothing.
  `unlock` is idempotent (extra calls no-op). The intended lifecycle is still
  single-threaded (lock once, unlock once); concurrent acquisition attempts
  on the SAME object are not coordinated — wrap if that's needed.

  A crash while locked leaves a stale `.lock` marker behind (the channel
  lock itself dies with the process, so correctness is unaffected). The
  marker is for humans only; don't trust it — the next successful `lock`
  overwrites it.

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
        a-held      (atom nil)   ; {:channel FileChannel :flock FileLock} while held
        ;; One non-blocking acquisition attempt. Opens the channel, tries the
        ;; lock, and on ANY non-acquired outcome closes the channel again, so
        ;; no path through here leaks. Returns :acquired | :other-process |
        ;; :this-process.
        try-acquire!
        (fn []
          (let [ch (FileChannel/open status-path
                                     (into-array StandardOpenOption
                                                 [StandardOpenOption/CREATE
                                                  StandardOpenOption/WRITE]))]
            (try
              (if-let [fl (.tryLock ch)]
                (do (reset! a-held {:channel ch :flock fl})
                    ;; Advisory human marker — best-effort by design: a spit
                    ;; failure (permissions, disk full) must never strand the
                    ;; coordination lock we just acquired.
                    (try (spit (str path-str ".lock") "locked")
                         (catch Exception _))
                    :acquired)
                (do (.close ch) :other-process))
              (catch OverlappingFileLockException _
                (.close ch)
                :this-process)
              (catch Throwable e
                (.close ch)
                (throw e)))))]
    (reify Lock
      (lock [_]
        (case (try-acquire!)
          :acquired nil
          :other-process
          (throw (ex-info (str "dj.recorder: log is locked by another process: " path-str)
                          {:path path-str :dj.recorder/locked-by :other-process}))
          :this-process
          (throw (ex-info (str "dj.recorder: log is already open in this process: " path-str
                               " — close! the existing db first (or you're re-opening in a"
                               " REPL without having closed the old handle)")
                          {:path path-str :dj.recorder/locked-by :this-process}))))
      (lockInterruptibly [this]
        ;; Acquisition is non-blocking, so there is no wait to interrupt.
        (.lock this))
      (tryLock [_]
        (= :acquired (try-acquire!)))
      (tryLock [this time unit]
        ;; Poll up to the deadline. Coarse (50ms) is fine: contention here is
        ;; whole-process, not hot-path.
        (let [deadline (+ (System/nanoTime) (.toNanos ^TimeUnit unit (long time)))]
          (loop []
            (cond
              (.tryLock this)                        true
              (<= (- deadline (System/nanoTime)) 0)  false
              :else (do (Thread/sleep 50) (recur))))))
      (newCondition [_]
        (throw (UnsupportedOperationException.
                "dj.recorder file-lock does not support conditions")))
      (unlock [_]
        ;; Idempotent: atomically take ownership of the held state; a second
        ;; unlock sees nil and no-ops.
        (when-let [{:keys [^FileChannel channel ^FileLock flock]}
                   (first (swap-vals! a-held (constantly nil)))]
          (try
            (.release flock)
            (finally
              (.close channel)
              (Files/deleteIfExists lock-path))))))))
