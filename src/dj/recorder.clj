(ns dj.recorder
  "dj.recorder — the public API / lifecycle for the alpha (item 4).

  A durable, crash-safe reference for *native* Clojure data structures: the
  read ergonomics of an atom (synchronous `deref`) with the durability of an
  append-only EDN log. The surface mirrors `atom`/`agent` on purpose (north
  star: \"an atom/agent you can trust across restarts\"; runtime deep-dive §1):

    (require '[dj.recorder :as r])
    (def db (r/open \"state.edn\"))      ; lock + rehydrate + ready the drainer
    @db                                  ; current realized state (instant; never blocks on I/O)
    (r/patch! db {:k v})                 ; enqueue a literal patch; returns the per-tx promise
    (r/tx! db (fn [s] {:k v}))           ; enqueue a read-modify-write authoring fn
    (r/update! db [:a :b] inc)           ; sugar: deep read-modify-write leaf
    (r/move! db [:xs] 0 2)               ; sugar: reorder a vector element
    (r/await db)                         ; block until the queue drains (agent-style)
    (r/close! db)                        ; drain, close the file, release the lock

  This namespace is a thin composition layer: it wires the single-writer file
  lock + torn-tail-aware rehydrate (storage, item 2) into the on-demand vthread
  drainer (dispatch, item 3). All the hard contracts live there; here we own
  lifecycle (open/close), the read-your-writes story, and the torn-tail policy.

  Contracts surfaced here (runtime deep-dive):
    §1  unit of change = a `state -> patch` fn; only the returned *data* patch
        is persisted. `tx!` takes such a fn; `patch!` is the literal-patch
        `(constantly patch)` arity. `update!`/`move!` are `tx!` sugar.
    §3  persist-then-publish: `@db` is the last *durably-written* state and may
        lag the queue. Read-your-writes is opt-in: `@(patch! …)`/`@(tx! …)` or
        `(await …)`.
    §4  torn tail on rehydrate is SURFACED by default (open raises with the
        offset + raw bytes); `:on-torn-tail :discard` truncates and proceeds.
    §5  a halted db (I/O failure) stays `deref`-able but rejects writes; recover
        via `close!` + re-`open`.
    §6  single-writer lock held for the db's life; `close!` drains before
        releasing so in-flight writes complete."
  (:refer-clojure :exclude [await])
  (:require [dj.recorder.storage :as storage]
            [dj.recorder.dispatch :as dispatch]
            [dj.recorder.patch :as patch])
  (:import [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; The db handle. Deref reads the realized state (persist-then-publish, §3);
;; the other fields are lifecycle internals (the dispatch core, the durable
;; writer, the single-writer lock, the log path, and a one-shot closed flag).
;; ---------------------------------------------------------------------------

(deftype Recorder [core writer lock path a-closed]
  clojure.lang.IDeref
  (deref [_] @core))

(defmethod print-method Recorder [^Recorder db ^java.io.Writer w]
  (.write w (str "#dj.recorder/db " (pr-str {:path (.-path db)
                                             :halted? (some? (dispatch/error (.-core db)))
                                             :closed? @(.-a-closed db)}))))

;; ---------------------------------------------------------------------------
;; open — lock, rehydrate, ready the drainer
;; ---------------------------------------------------------------------------

(defn open
  "Open the log at `path` and return a `Recorder` (a `deref`-able db handle).

  Acquires the single-writer file lock, replays the log onto `baseline` (the
  *same* `apply-patch` fold the drainer uses), and wires a durable `append!`
  into a fresh dispatch core. `@db` then reads the rehydrated state instantly.

  opts (a map; all optional):
    :baseline      initial value to fold the log onto (default `{}`). Root state
                   is arbitrary EDN, so a caller may seed e.g. `[]` or a record.
    :on-torn-tail  policy for a torn trailing record left by a crash (§4):
                     :surface (default) — raise an ex-info carrying the byte
                       offset + raw partial bytes/text so you can inspect the
                       lost in-flight tx before deciding. The log is left intact.
                     :discard — truncate the file back to the last good record
                       and proceed (the convenient path once you've decided the
                       lost tx doesn't matter).

  On any failure during open (torn-tail surface, I/O), the file lock is released
  before the exception propagates, so a failed open never leaks the lock. A
  missing file opens a fresh db at `baseline`."
  ([path] (open path {}))
  ([path {:keys [baseline on-torn-tail]
          :or   {baseline {} on-torn-tail :surface}}]
   (let [lock (storage/file-lock path)]
     (.lock ^Lock lock)
     (try
       (let [{:keys [state torn-tail]} (storage/read-log baseline path)]
         (when torn-tail
           (case on-torn-tail
             ;; Truncate the partial away; `state` already excludes it (read-log
             ;; only replayed the good prefix), so it stays correct as-is.
             :discard (storage/truncate-to path (:offset torn-tail))
             :surface (throw (ex-info (str "dj.recorder: torn trailing record at " path
                                           " (byte offset " (:offset torn-tail)
                                           ") — the in-flight tx lost to a crash. Inspect the ex-data's"
                                           " :dj.recorder/torn-tail — read :text for the fragment (a tear"
                                           " mid-multibyte-char shows a trailing �; :bytes has the raw"
                                           " bytes). Then re-open with {:on-torn-tail :discard} to drop it.")
                                      {:dj.recorder/torn-tail torn-tail :path (str path)}))
             (throw (ex-info "dj.recorder: unknown :on-torn-tail policy (want :surface or :discard)"
                             {:on-torn-tail on-torn-tail}))))
         (let [writer  (storage/open-writer path)
               append! (fn [patch] (storage/append! writer patch))
               core    (dispatch/make-core state append!)]
           (->Recorder core writer lock path (atom false))))
       (catch Throwable e
         (.unlock ^Lock lock)                  ; don't leak the lock on a failed open
         (throw e))))))

;; ---------------------------------------------------------------------------
;; patch! / tx! — enqueue a change
;;
;; The unit of change is a `state -> patch` fn (§1); only the returned *data*
;; patch is ever persisted (a closure is never stored — patch-sketch §7.5). The
;; two entry points differ only in what the caller supplies:
;;   • `patch!` — a literal patch value (data), enqueued as `(constantly patch)`.
;;   • `tx!`    — a `state -> patch` authoring fn, for read-modify-write.
;; Splitting them (vs. one `fn?`-dispatching entry) means data that happens to be
;; callable — records/maps implementing IFn — is never mistaken for an authoring
;; fn, and each name reads for exactly one job.
;; ---------------------------------------------------------------------------

(defn- enqueue!
  "Guard the closed flag, then hand a `state -> patch` fn to the drainer (§1).
  Returns the per-tx promise: it resolves to the new realized state on success,
  or to a `Throwable` on a patch/authoring or I/O error (the error channel §5).
  Throws if the db is closed, or if it is halted by an earlier I/O failure."
  [^Recorder db f]
  (when @(.-a-closed db)
    (throw (ex-info "dj.recorder: db is closed" {:dj.recorder/closed true :path (str (.-path db))})))
  (dispatch/submit! (.-core db) f))

(defn patch!
  "Enqueue a literal `patch` (plain data) and return its per-tx promise (§1/§5).

  The patch is applied against the latest realized state on the drainer and the
  *data* itself is persisted verbatim. Use `tx!` when the patch must be computed
  from current state (read-modify-write).

  The returned promise is both the sync barrier and the error channel: it
  resolves to the new realized state on success, or to a `Throwable` on a
  patch/I/O error. Fire-and-forget = ignore it; sync = deref it (and branch on
  `Throwable`). Because of persist-then-publish, `@db` may lag a fire-and-forget
  write — to read your own write, deref the promise or `await` first (§3)."
  [^Recorder db patch]
  (enqueue! db (constantly patch)))

(defn tx!
  "Enqueue a `state -> patch` authoring fn `f` and return its per-tx promise
  (§1/§5). `f` is run serialized on the drainer against the latest realized
  state, so read-modify-write is race-free (§9); only the *data* patch it
  returns is persisted (a returned `nil` is a no-op). See `patch/update-in` and
  `patch/move` for building the returned patch; `update!`/`move!` wrap the common
  cases. Same promise/error-channel semantics as `patch!`."
  [^Recorder db f]
  (enqueue! db f))

(defn update!
  "Read-modify-write a deep leaf: sugar for
  `(tx! db (fn [s] (apply patch/update-in s path f args)))`. Reads `(get-in s
  path)`, applies `f`, and persists the overwritten leaf (so a shrinking `f`
  like `dissoc` is reflected). `path` may be empty to update the root. Returns
  the per-tx promise."
  [db path f & args]
  (tx! db (fn [s] (apply patch/update-in s path f args))))

(defn move!
  "Relocate the element of the vector at `path` from index `from` to index `to`:
  sugar for `(tx! db (fn [s] (patch/move s path from to)))`. `to` is the
  element's final 0-based index; `from` = `to` is a no-op. `path` may be empty
  when the root is the vector. Returns the per-tx promise."
  [db path from to]
  (tx! db (fn [s] (patch/move s path from to))))

;; ---------------------------------------------------------------------------
;; await / halted / close!
;; ---------------------------------------------------------------------------

(defn await
  "Block until every change enqueued before this call has been processed
  (agent-style `await`; the read-your-writes barrier — §3). Returns nil."
  [^Recorder db]
  (dispatch/await (.-core db))
  nil)

(defn halted
  "The `Throwable` that halted this db via an I/O failure, or nil if it is still
  live (§5). A halted db is still `deref`-able but rejects writes; recover by
  `close!` + re-`open`."
  [^Recorder db]
  (dispatch/error (.-core db)))

(defn close!
  "Drain in-flight work, close the writer, and release the single-writer lock
  (§6). Idempotent: the first call wins (a one-shot flag), later calls no-op.
  After close, `patch!`/`tx!` throw but `@db` still works (the db becomes an
  immutable view of its last state — durable2's close-flag model). Returns nil."
  [^Recorder db]
  (when (compare-and-set! (.-a-closed db) false true)
    (dispatch/await (.-core db))                    ; let queued writes complete
    (.close ^java.lang.AutoCloseable (.-writer db))
    (.unlock ^Lock (.-lock db)))
  nil)
