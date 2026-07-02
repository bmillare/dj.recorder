(ns dj.recorder.protocols
  "The internal contracts for dj.recorder's layers: the storage append log
  (`AppendLog`) and the dispatch core (`IRecorderCore`). The `open-writer`
  reify (storage) and the `Core` deftype (dispatch) are the implementors.

  Behavioural contracts referenced as §N are detailed in the runtime deep-dive
  (ledger/2026-06-27-runtime-design-deep-dive.md); the API shape and the locked
  decisions live in live/north_star.md. Mirrors dj.durable.protocols."
  (:refer-clojure :exclude [await]))

(defprotocol AppendLog
  (append! [log patch]
    "Append one patch to the log as a single EDN line and flush. The flush
    pushes to the OS page cache — durable across a process crash, not
    necessarily across power loss (see dj.recorder.storage's ns docstring;
    §4). Throws (leaving the log untouched) if `patch` does not round-trip
    as EDN."))

(defprotocol IRecorderCore
  (submit! [core patch-fn]
    "Enqueue a `state → patch` authoring fn and ensure a drainer is running.
    Returns the per-tx promise: it resolves to the new realized state on
    success, or to a `Throwable` on a patch/authoring or I/O error (§5).
    Submission order is commit order. Throws if the core is halted — writes
    throw on a halted db; reads (`deref`) still work. Recover via the public
    lifecycle's close!/re-open. Also throws once quiesce! has sealed the core
    (closed db). Enqueue, halted-check, and sealed-check are one atomic swap,
    so a submit can never be stranded by a concurrent halt nor slip in behind
    a close.")
  (await [core]
    "Block until every tx queued *before this call* has been processed
    (agent-style `await`). Implemented as a first-class barrier item: it is
    never authored, applied, or persisted — its promise resolving simply means
    all prior txs drained. Returns nil. Throws the halted ex-info (same shape
    as `submit!`, cause = the halting Throwable) if the core is already halted
    OR halts before the barrier drains — matching `clojure.core/await`, which
    throws on a failed agent — and the closed ex-info on a sealed core (no
    barrier is enqueued, so no stray drainer is ever spawned post-close).
    `await` returning nil therefore *guarantees* everything queued before it
    is durable.")
  (error [core]
    "The Throwable that halted this core (I/O failure, VM error, or a dispatch
    bug caught by the drainer backstop — §5), or nil if it is still live.
    Named after `agent-error`."))
