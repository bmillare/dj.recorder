(ns dj.recorder
  "dj.recorder — the missing middle for lightweight state persistence.

  A durable, crash-safe reference for *native* Clojure data structures:
  the read ergonomics of an atom (synchronous `deref`) with the durability
  of a database, achieved with an append-only delta log + background
  compaction, and no native/embedded-DB dependencies.

  See the agent workspace for the full design:
    live/north_star.md        — objective, scope, chosen approach
    live/gaps.md              — conceptual nodes + open blind spots
    ledger/tactical_ideation.md — per-decision implementation options

  This is an intentionally empty seed namespace; no API is committed yet.")

(comment
  ;; Smoke check that the namespace loads and the toolchain is wired up.
  (+ 1 2 3))
