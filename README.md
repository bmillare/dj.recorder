# dj.recorder

The missing middle for lightweight state persistence in Clojure.

A durable, crash-safe reference for **native Clojure data structures** — the
synchronous read ergonomics of an atom (`@db`) with the durability of an
append-only, human-readable EDN delta log. No native or embedded-database
dependencies (pure JVM/Clojure).

```clojure
(require '[dj.recorder :as r])

(def db (r/open "state.edn"))         ; lock + rehydrate the log; ready the drainer
@db                                   ; current state — instant, never blocks on I/O
(r/patch! db {:user {:name "Ada"}})   ; append a literal patch; returns a per-tx promise
(r/tx! db (fn [s] ...))               ; read-modify-write authoring fn (race-free)
(r/update! db [:a :b] inc)            ; deep-leaf RMW sugar
(r/move! db [:xs] 0 2)                ; reorder a vector element
@(r/patch! db {:user {:age 30}})      ; read-your-writes: deref -> new state (throws on failure)
(r/await db)                          ; block until the queue drains (agent-style)
(r/close! db)                         ; drain, close the file, release the lock
```

## Working with the API directly

The API is meant to be used *directly* — reach for `@db` and `@(r/patch! …)`
the way you would an atom. Two things worth internalizing so you don't wrap it
in boilerplate:

**Reading your own write is just `@`.** Every write (`patch!`/`tx!`/`update!`/
`move!`) returns a per-tx promise. Dereferencing it blocks until the change is
durable on disk, then yields the new realized state — or **throws** the
underlying error (a bad patch, an I/O halt) the way a `future` does. So the
synchronous "do it, and throw if it failed" path needs no helper:

```clojure
@(r/update! db [:tracks "strobe" :plays] (fnil inc 0))   ; -> new state, or throws
```

Fire-and-forget just ignores the promise. To *inspect* a failure without
throwing (e.g. after a halt), use `(r/error db)` or the thrown ex-info's
`ex-data` — you don't need to unwrap the promise yourself.

**Prefer one transaction over several writes.** A patch can describe many
changes at once, so a single logical event should be a single `tx!`/`patch!` —
one durable append, applied atomically — rather than a sequence of writes each
waiting on durability:

```clojure
;; one event, one durable tx (not two patch! calls back-to-back)
@(r/tx! db (fn [_]
             {:current-run run-id
              :runs {run-id {:id run-id :status :started :steps [{:event :started}]}}}))
```

> Status: **alpha.** The core is built and tested — an append-only EDN log with
> torn-tail-aware rehydrate and a single-writer file lock, an on-demand
> virtual-thread dispatcher with persist-then-publish ordering and
> halt-on-I/O-failure semantics, and a deep *additive* patch algebra (with
> `#dj.recorder/replace` / `dissoc` / `#dj.recorder/splice` escapes). The public
> API above is not yet frozen, and compaction (delta-log roll-up) is designed
> but not yet implemented.

## Develop

Toolchain via a Nix flake (JDK + Clojure CLI + babashka):

```bash
nix develop                       # enter dev shell
clojure -M:nrepl                  # start an nREPL server (dynamic port)
clojure -M:test                   # run the test suite
```

## Layout

```
src/dj/recorder.clj            <- public API / lifecycle (open/patch!/tx!/await/close!)
src/dj/recorder/patch.clj      <- the patch algebra (additive merge + escapes)
src/dj/recorder/dispatch.clj   <- the single-drainer dispatch core
src/dj/recorder/storage.clj    <- append-only EDN log + file lock + rehydrate
src/dj/recorder/protocols.clj  <- internal protocols (AppendLog, IRecorderCore)
src/dj/recorder/adapter/       <- opt-in adapters: let other libs use dj.recorder
                                  as a durable backend (e.g. adapter/concurrency.clj)
deps.edn                       <- deps + :nrepl / :test aliases
flake.nix                      <- pure-JVM dev shell
```

`dj.durable2` is earlier prototype code kept in-tree for design lineage; the
shipping path is `dj.recorder`.

Adapters under `dj.recorder.adapter.*` are **opt-in**: dj.recorder's own `:deps`
stay empty, and requiring `dj.recorder` never loads them. Each implements
*another* library's protocol so that library can use a dj.recorder db as its
durable store (cf. `ring.adapter.*`). Only code that wants one pulls both
libraries onto the classpath and requires the adapter ns.

## License

Copyright © 2026 Brent Millare

Distributed under the [Eclipse Public License 2.0](LICENSE) (EPL-2.0), the same
license as Clojure.
