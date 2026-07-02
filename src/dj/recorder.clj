(ns dj.recorder
  "dj.recorder — the public API / lifecycle.

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
  lock + torn-tail-aware rehydrate (storage) into the on-demand vthread drainer
  (dispatch). All the hard contracts live there; here we own lifecycle
  (open/close), the read-your-writes story, and the torn-tail policy.

  Contracts surfaced here (detailed in the runtime deep-dive §1/§3–§6;
  summarized in live/north_star.md):
    §1  unit of change = a `state -> patch` fn; only the returned data patch is
        persisted. `patch!` is the `(constantly patch)` arity; `update!`/`move!`
        are `tx!` sugar.
    §3  persist-then-publish: `@db` is the last durably-written state and may lag
        the queue; read-your-writes is opt-in (deref the promise or `await`).
    §4  a torn tail on rehydrate is surfaced by default; `:discard` truncates.
    §5  a halted db stays `deref`-able but rejects writes (inspect via `error`).
    §6  single-writer lock held for the db's life; `close!` seals the core
        (quiesce!) then releases the lock in a `finally`."
  (:refer-clojure :exclude [await])
  (:require [dj.recorder.storage :as storage]
            [dj.recorder.dispatch :as dispatch]
            [dj.recorder.protocols :as proto]
            [dj.recorder.patch :as patch])
  (:import [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; The db handle. Deref reads the realized state (persist-then-publish, §3)
;; straight off the dispatch core's state atom — held directly here because we
;; own both sides of that boundary; the contract (last durably-written state,
;; single-writer drainer, volatile-safe) is pinned in dispatch. The other
;; fields are lifecycle internals.
;; ---------------------------------------------------------------------------

(deftype Recorder [core      ; the dispatch core (submit!/await/error/quiesce!)
                   state     ; the core's state atom — `@db` reads it directly
                   writer lock path a-closed]
  clojure.lang.IDeref
  (deref [_] @state))

(defmethod print-method Recorder [^Recorder db ^java.io.Writer w]
  (.write w (str "#dj.recorder/db " (pr-str {:path (.-path db)
                                             :halted? (some? (proto/error (.-core db)))
                                             :closed? @(.-a-closed db)}))))

;; ---------------------------------------------------------------------------
;; open — lock, rehydrate, ready the drainer
;; ---------------------------------------------------------------------------

(defn open
  "Open the log at `path` and return a `Recorder` (a `deref`-able db handle).

  Acquires the single-writer file lock (fail-fast — throws immediately if the
  log is already locked, whether by another process or an unclosed handle in
  this one, rather than blocking), replays the log onto `baseline` (the *same*
  `apply-patch` fold the drainer uses), and wires a durable `append!` into a
  fresh dispatch core. `@db` then reads the rehydrated state instantly.

  opts (a map; all optional):
    :baseline      initial value to fold the log onto (default `{}`). Root state
                   is arbitrary EDN, so a caller may seed e.g. `[]` or a record.
                   (An explicit nil is indistinguishable from absent and gets
                   the default.)
    :on-torn-tail  policy for a torn trailing record left by a crash (§4):
                     :surface (default) — raise an ex-info carrying the byte
                       offset + raw partial bytes/text so you can inspect the
                       lost in-flight tx before deciding. The log is left intact.
                     :discard — truncate the file back to the last good record
                       and proceed (the convenient path once you've decided the
                       lost tx doesn't matter).

  On any failure during open (torn-tail surface, I/O), the file lock is released
  — and the writer, if already opened, closed — before the exception propagates,
  so a failed open never leaks either. A missing file opens a fresh db at
  `baseline`."
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
         (let [writer (storage/open-writer path)]
           (try
             (let [append!    (fn [patch] (proto/append! writer patch))
                   state-atom (atom state)
                   core       (dispatch/make-core state-atom append!)]
               (->Recorder core state-atom writer lock path (atom false)))
             (catch Throwable e
               (.close ^java.lang.AutoCloseable writer)  ; don't leak the writer either
               (throw e)))))
       (catch Throwable e
         (.unlock ^Lock lock)                  ; don't leak the lock on a failed open
         (throw e))))))

;; ---------------------------------------------------------------------------
;; tx! / patch! — enqueue a change
;;
;; The unit of change is a `state -> patch` fn (§1); only the returned *data*
;; patch is ever persisted (a closure is never stored — patch-sketch §7.5).
;; `tx!` is the single write-path choke point (the closed-check lives here and
;; nowhere else); `patch!` is literally `tx!` of `(constantly patch)`. Keeping
;; them as two names (vs. one `fn?`-dispatching entry) means data that happens
;; to be callable — records/maps implementing IFn — is never mistaken for an
;; authoring fn, and each name reads for exactly one job.
;; ---------------------------------------------------------------------------

(defn tx!
  "Enqueue a `state -> patch` authoring fn `f` and return its per-tx promise
  (§1/§5). `f` is run serialized on the drainer against the latest realized
  state, so read-modify-write is race-free (§9); only the *data* patch it
  returns is persisted (a returned `nil` is a no-op). See `patch/update-in` and
  `patch/move` for building the returned patch; `update!`/`move!` wrap the
  common cases.

  The returned promise is both the sync barrier and the error channel: it
  resolves to the new realized state on success, or to a `Throwable` on a
  patch/authoring or I/O error. Fire-and-forget = ignore it; sync = deref it
  (and branch on `Throwable`). Because of persist-then-publish, `@db` may lag a
  fire-and-forget write — to read your own write, deref the promise or `await`
  first (§3).

  Throws if the db is closed or halted (§5/§6). The check here is the friendly
  fast path; the authoritative guard is the dispatch core itself, which is
  atomically sealed by close! — a submit racing close! is refused there, so no
  write can ever land behind the final drain."
  [^Recorder db f]
  (when @(.-a-closed db)
    (throw (ex-info "dj.recorder: db is closed"
                    {:dj.recorder/closed true :path (str (.-path db))})))
  (proto/submit! (.-core db) f))

(defn patch!
  "Enqueue a literal `patch` (plain data) and return its per-tx promise: sugar
  for `(tx! db (constantly patch))` — and implemented as exactly that (§1/§5).

  The patch is applied against the latest realized state on the drainer and the
  *data* itself is persisted verbatim. Use `tx!` when the patch must be computed
  from current state (read-modify-write). Same promise/error-channel semantics
  as `tx!`."
  [^Recorder db patch]
  (tx! db (constantly patch)))

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
;; await / error / close!
;; ---------------------------------------------------------------------------

(defn await
  "Block until every change enqueued before this call has been processed
  (agent-style `await`; the read-your-writes barrier — §3). Returns nil — and a
  returning `await` therefore *guarantees* every prior write is durable. Throws
  the halted ex-info (agent parity with `clojure.core/await` on a failed agent)
  if the db is already halted or halts before this call's work drains (§5), and
  the closed ex-info on a closed db (the sealed core refuses the barrier — no
  stray drainer is ever spawned post-close). For a halt-tolerant drain, close!
  handles it."
  [^Recorder db]
  (proto/await (.-core db))
  nil)

(defn error
  "The `Throwable` that halted this db via an I/O failure, or nil if it is still
  live (§5). Named after `agent-error`, matching `proto/error`. A halted db
  is still `deref`-able but rejects writes; recover by `close!` + re-`open`."
  [^Recorder db]
  (proto/error (.-core db)))

(defn close!
  "Drain in-flight work, close the writer, and release the single-writer lock
  (§6). Idempotent: the first call wins (a one-shot flag), later calls no-op.

  Drains through `dispatch/quiesce!`, which in ONE atomic swap seals the core
  (all further submits/awaits refuse — including any writer that raced past
  this namespace's closed-flag check) and enqueues the final barrier, then
  *joins* the drainer thread (never interrupts; §7). On its return the writer
  is provably quiet and safe to close. quiesce! is halt-safe and idempotent, so
  a db halted by an earlier I/O failure still closes cleanly (it simply has
  nothing left to drain — unlike `await`, close! never throws on a halted db).

  The lock is released in a `finally`: even if quiesce! or the writer's close
  throws, the single-writer lock is never stranded (safe — the drainer is
  already retired by the time either can throw). After close, `patch!`/`tx!`/
  `await` throw but `@db` still works (the db becomes an immutable view of its
  last state — durable2's close-flag model). Returns nil."
  [^Recorder db]
  (when (compare-and-set! (.-a-closed db) false true)
    (try
      (dispatch/quiesce! (.-core db))               ; seal + drain + retire (§6/§7)
      (.close ^java.lang.AutoCloseable (.-writer db))
      (finally
        (.unlock ^Lock (.-lock db)))))
  nil)
