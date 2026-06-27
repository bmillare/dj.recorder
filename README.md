# dj.recorder

The missing middle for lightweight state persistence in Clojure.

A durable, crash-safe reference for **native Clojure data structures** — the
synchronous read ergonomics of an atom with the durability of a database,
backed by an append-only delta log and background compaction, with **no native
or embedded-database dependencies** (pure JVM/Clojure).

> Status: **seed**. Design is settled enough to start; no public API is
> committed yet. The authoritative design lives in the agent workspace
> (`../agent/live/north_star.md`, `../agent/live/gaps.md`,
> `../agent/ledger/tactical_ideation.md`).

## Develop

This repo provides its toolchain via a Nix flake (JDK + Clojure CLI + babashka):

```bash
nix develop                       # enter dev shell
clojure -M:nrepl                  # start an nREPL server (dynamic port)
```

Agents evaluate forms against the running nREPL via `clj-nrepl-eval`
(see `../agent/ledger/2026-06-16-clojure-repl-light-workflow.md`).

Run tests (once they exist):

```bash
clojure -M:test
```

## Layout

```
src/dj/recorder.clj   <- seed namespace
deps.edn              <- deps + :nrepl / :test aliases
flake.nix             <- pure-JVM dev shell
```
