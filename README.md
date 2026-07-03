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
(r/await db)                          ; block until the queue drains (agent-style)
(r/close! db)                         ; drain, close the file, release the lock
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
deps.edn                       <- deps + :nrepl / :test aliases
flake.nix                      <- pure-JVM dev shell
```

`dj.durable2` is earlier prototype code kept in-tree for design lineage; the
shipping path is `dj.recorder`.

## License

Copyright © 2026 Brent Millare

Distributed under the [Eclipse Public License 2.0](LICENSE) (EPL-2.0), the same
license as Clojure.
