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

## The patch format (deep dive)

A **patch** is the unit of change in dj.recorder, and it's the idea worth
understanding — the durable log is almost a thin wrapper on top of it. A patch
is just **plain EDN data that describes how to transform one Clojure value into
the next**, and applying one is a single pure function:

```clojure
(require '[dj.recorder.patch :as p])

(p/apply-patch {:user {:name "Ada" :age 30}}   ; current value
               {:user {:age 31}})              ; patch (plain data)
;; => {:user {:name "Ada" :age 31}}
```

That function is the whole engine. A db is nothing more than an append-only log
of patches plus `(reduce apply-patch baseline log)` to fold them back into state
on open — the *same* fold the write thread uses to advance live state (one code
path for memory and disk; a divergence there would be a corruption bug). You can
also call `apply-patch` entirely on its own, as a generic "apply a described
change" for native Clojure data — no db, no file, no I/O in sight.

### The one rule: an untagged patch is additive

Merging *adds*; it never shrinks or overwrites in place. What "add" means is
dispatched on the current value:

| current value | an untagged patch means |
| :-- | :-- |
| map / record | merge keys, recursing into each value |
| set | union (add members) |
| vector | concat — append the patch's elements |
| list / seq | concat (append) |
| scalar | replace (nothing to add to) |
| `nil` *(as a patch)* | **identity — no change at all** |

Recursion bottoms out on scalars, so cost scales with the *change*, not the size
of the state. Records keep map semantics (an untagged map merges into them and
the record type is preserved). Incompatible merges — e.g. a map patch against a
scalar, or a vector patch against a map — **throw**, on the theory that it's
almost always a mistake; reach for `#dj.recorder/replace` when you genuinely mean
"change the shape."

`nil` is worth pausing on: as a *patch* it means *no change*, everywhere. That
makes a `state -> patch` function which returns `nil` (the natural "nothing to
do" reflex) a safe no-op instead of nuking the root. The three intents people
tend to conflate stay distinct:

- **no change** → `nil`
- **remove a key** → `dissoc` (below)
- **store a literal `nil`** → `#dj.recorder/replace nil`

### The escapes: `replace`, `dissoc`, `splice`

Additive merge covers most edits; three tags cover the rest.

**Overwrite** — `#dj.recorder/replace x` sets the value to `x` verbatim, no
merge. Universal: replace a whole subtree, shrink a vector, change a value's
shape (scalar → map), or store data that would otherwise look like a marker.

**Remove** — two spellings of one idea:
- *inline* (map keys / vector indices): the sentinel `:dj.recorder/dissoc` as a
  **value** in a merge map deletes that key/index. Ergonomic for "update some
  keys, delete others" in one map literal.
- *op form* (sets / bulk): `#dj.recorder/dissoc <coll>` removes the listed keys
  (from a map) or members (from a set).

**Splice** — `#dj.recorder/splice [hunk ...]` does ordered positional vector
edits (insert / delete / range-replace), modeled on a unix unified-diff hunk.
Each hunk is `{:at i :- n :+ [..]}`: start index into the **original** vector,
count removed, elements inserted. `{:at i :+ [..]}` is a pure insert,
`{:at i :- n}` a pure delete, both together a range-replace. Hunks are all
original-relative and must not overlap (they're applied high-index-first, so no
offset bookkeeping is needed).

### Worked examples

Read each line as `current-value  ⟵ patch ⟶  result`:

```clojure
;; maps — merge, recurse into values, delete inline
{:a 1 :b 2}                     {:b 3 :c 4}                   => {:a 1 :b 3 :c 4}
{:user {:name "Bob" :age 30}}   {:user {:age 31}}             => {:user {:name "Bob" :age 31}}
{:a 1 :b 2}                     {:b :dj.recorder/dissoc}      => {:a 1}

;; nil = no change (NOT "store nil", NOT "delete")
{:a 1}                          {:a nil}                      => {:a 1}
{:a 1}                          {:a #dj.recorder/replace nil} => {:a nil}

;; replace a whole subtree (escape) — :age is dropped
{:user {:name "Bob" :age 30}}   {:user #dj.recorder/replace {:name "Alice"}}
                                                              => {:user {:name "Alice"}}

;; sets — union / remove members
#{:a :b}                        #{:b :c}                      => #{:a :b :c}
#{:a :b :c}                     #dj.recorder/dissoc #{:b}     => #{:a :c}

;; vectors — append / in-place update / index removal / splice
[1 2 3]                         [4 5]                         => [1 2 3 4 5]
[{:name "Bob"} {:name "Carol"}] {0 {:name "Alice"}}           => [{:name "Alice"} {:name "Carol"}]
[10 20 30]                      {1 :dj.recorder/dissoc}       => [10 30]
[10 30]                         #dj.recorder/splice [{:at 1 :+ [20]}]         => [10 20 30]
[10 20 30]                      #dj.recorder/splice [{:at 1 :- 1 :+ [:a :b]}] => [10 :a :b 30]

;; the root can be any EDN — a scalar patch overwrites; a shape change is explicit
41                              42                            => 42
42                              #dj.recorder/replace {:a 1}   => {:a 1}
```

### Authoring helpers

Hand-nesting a patch gets tedious for a deep read-modify-write or a reorder, so
two constructors build the patch for you. Both **read the current value and
return a plain-data patch** (no function is ever stored). Call them inside a
`tx!` authoring fn, where `s` is the race-free dispatch-thread state:

```clojure
;; overwrite a deep leaf with (f old-leaf & args) — like clojure.core/update-in,
;; but yields a minimal nested patch:
(p/update-in s [:tracks "strobe" :plays] (fnil inc 0))
;; => {:tracks {"strobe" {:plays 1}}}

;; relocate a vector element from one index to another (emits a #splice):
(p/move s [:crates "main"] 0 2)
```

`update!` / `move!` on the main API are one-liners over exactly these. Unlike
additive merge, `update-in` *overwrites* its leaf, so a shrinking `f` (`dissoc`,
`pop`) is reflected.

### Using it standalone

Nothing above requires the log. `dj.recorder.patch/apply-patch` — and
`rehydrate`, i.e. `(reduce apply-patch baseline patches)` — is pure and
dependency-free: a portable way to describe and apply a change to native Clojure
data, whether or not you persist it. To read the tagged literals back from text
(or EDN you stored yourself), register the reader tags:

- for the **Clojure reader**: they live in `resources/data_readers.clj`, loaded
  automatically when dj.recorder is on the classpath.
- for **`clojure.edn/read`**: pass `dj.recorder.patch/data-readers` as the
  `:readers` option (log replay uses `edn/read`, so it wires these in explicitly).

One caveat if you `pr-str` patches yourself: the markers honor
`*print-length*` / `*print-level*`, so bind both to `nil` first (the log writer
already does) — otherwise a REPL-configured truncation can silently corrupt the
printed patch.

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
clojure -M:test                   # run the core suite (dependency-free)
clojure -M:adapter                # opt-in adapter smoke test (pulls in dj.concurrency)
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
libraries onto the classpath and requires the adapter ns. Their tests live in
`test-adapter/` (outside the core suite) and run only via `-M:adapter`, which is
the sole place the adapted library is pulled onto the classpath — so the core
suite stays fast and dependency-free.

## License

Copyright © 2026 Brent Millare

Distributed under the [Eclipse Public License 2.0](LICENSE) (EPL-2.0), the same
license as Clojure.
