# dj.recorder

The missing middle for state persistence in Clojure. 

`dj.recorder` gives you the crash-safe durability of a database, with the zero-friction ergonomics of a standard Clojure `atom`. It backs your native Clojure data structures with an append-only, human-readable EDN log. Zero dependencies; pure JVM/Clojure.

> Status: alpha. The core is built, tested, and crash-safe.

## The problem

You're writing a Clojure app, a REPL script, or a long-running data pipeline. You need to save state so that if the JVM crashes or you restart your REPL, you don't lose your work. 

Your options usually suck:
- **Use a database (Postgres, SQLite, Datomic):** Massive overkill. Now you have schemas, connections, and external dependencies for a script that just needs to remember a hash map.
- **`spit` / `slurp` a file:** Easy, but dangerous. If your app crashes mid-write, the file corrupts. If multiple threads write at once, you get race conditions. And writing a massive 50MB map to disk on every minor update destroys performance.

## The payoff

`dj.recorder` solves this by giving you a durable reference that feels exactly like reading an `atom`, while writing changes to disk incrementally in the background.

```clojure
(require '[dj.recorder :as r])

;; 1. Open the file. If it exists, state is instantly rehydrated.
(def db (r/open "state.edn"))

;; 2. Read state exactly like an atom. This is instantaneous in-memory; zero I/O blocking.
@db  
;;=> nil (or your previous state)

;; 3. Describe your changes as simple EDN patches. 
;; The engine intelligently merges them.
(r/patch! db {:last-run "2024-10-25" :status :running})
(r/patch! db {:status :finished, :metrics {:items-processed 42}})

;; 4. Read the updated state.
@db
;;=> {:last-run "2024-10-25" :status :finished :metrics {:items-processed 42}}
```

Behind the scenes:
- Reads (`@db`) are instantly served from memory.
- Writes (`patch!`) are safely queued and appended to `state.edn` as a human-readable delta log.
- **Crash-safe:** If the power goes out, your file is never corrupted because it's append-only. When you restart, `r/open` replays the log instantly.

## Usage

### Safe Mutations (Read-Modify-Write)

`r/patch!` is great for literal data updates. But what if you need to increment a counter or modify a list based on the *current* state? 

Because writes happen in the background, `dj.recorder` serializes updates to prevent race conditions. You can use `r/tx!` to safely author a change based on the current state (much like a Clojure agent).

```clojure
;; tx! takes a function: (fn [current-state] patch)
(r/tx! db (fn [state]
            {:run-count (inc (get state :run-count 0))}))
```

For deep updates, `dj.recorder` provides convenient sugar out of the box:

```clojure
;; Update a deep leaf safely (like update-in)
(r/update! db [:metrics :errors] (fnil inc 0))

;; Reorder items in a vector
(r/move! db [:playlist] 0 2)
```

### Read-Your-Writes & Synchronous Guarantees

`dj.recorder` guarantees **persist-then-publish**: `deref`ing the database (`@db`) will *never* show you data that hasn't been safely written to disk.

By default, mutations are asynchronous. But every write returns a promise. If you need to ensure the data is safely on disk before your program moves on, simply `deref` (`@`) the write. If the disk fails, this throws an error—exactly like a Clojure `future`.

```clojure
;; Blocks until safely written to disk, then returns the newly realized state
@(r/patch! db {:user {:name "Ada" :age 30}})
;;=> {:user {:name "Ada" :age 30}}
```

### Initialization & Shutting Down

When opening a recorder, you can provide a `:baseline` state (defaults to `{}`).
```clojure
(def db (r/open "state.edn" {:baseline {:status :init}}))
```

When your app shuts down, you should cleanly close the recorder. This safely flushes all pending writes, joins the background writer thread, and releases the file lock.
```clojure
(r/close! db) ; Idempotent, returns nil
```

*(Need to wait for the write queue to drain without closing the file? Use `(r/await db)`).*

---

## Deep Dive: The Patch Format

A **patch** is the core unit of change. Rather than saving your *entire* 50MB state every time you make a change, the log only appends *what changed*. 

To ensure the log remains pure EDN, `dj.recorder` validates all patches. You cannot save functions or live DB connections.

The core rule of patching is simple: **untagged patches are purely additive.** 

| Current State | Patch | Result |
| :-- | :-- | :-- |
| `{:a 1}` | `{:b 2}` | `{:a 1, :b 2}` (Maps merge) |
| `#{:a}` | `#{:b}` | `#{:a, :b}` (Sets union) |
| `[1 2]` | `[3 4]` | `[1 2 3 4]` (Vectors concat) |

**Crucial Note:** `nil` is the identity patch. It means "no change". Sending `{:a nil}` will *not* delete `:a`. 

This pure additive math means the cost to write scales with the *size of the change*, not the size of your total state. 

### Overwrites, Deletions, and Splices

Because untagged patches are purely additive, `dj.recorder` provides three special tags (EDN literals) when you genuinely want to remove or overwrite data:

1. **Overwrite (`#dj.recorder/replace`)**: Replaces the value entirely, ignoring merge rules. Useful for storing literal `nil`s or replacing a whole subtree.
   ```clojure
   ;; Replace a map rather than merging into it
   (p/apply-patch {:user {:name "Bob" :age 30}}
                  {:user #dj.recorder/replace {:name "Alice"}})
   ;;=> {:user {:name "Alice"}}
   ```

2. **Remove (`:dj.recorder/dissoc`)**: Deletes a key from a map or an index from a vector.
   ```clojure
   (p/apply-patch {:a 1 :b 2} {:b :dj.recorder/dissoc})
   ;;=> {:a 1}
   ```

3. **Splice (`#dj.recorder/splice`)**: Advanced positional edits for vectors (insert / delete / replace ranges).
   ```clojure
   (p/apply-patch [10 20 30] #dj.recorder/splice [{:at 1 :- 1 :+ [:a :b]}])
   ;;=> [10 :a :b 30]
   ```

*You can test all of these behaviors locally without touching a database using the pure function `dj.recorder.patch/apply-patch`.*

## Deep Dive: Crash Recovery 

Because `dj.recorder` uses an append-only log, it is incredibly resilient to crashes. But what happens if someone trips over the power cord at the exact microsecond a patch is being written to disk?

You get a "torn tail"—a file that ends in malformed EDN.

By default, `r/open` uses `{:on-torn-tail :surface}`. It will throw an exception on startup so you can manually inspect the corrupted bytes. 

If you want the database to automatically self-heal, you can tell it to simply discard the partial write and safely resume from the last known-good state:

```clojure
(def db (r/open "state.edn" {:on-torn-tail :discard}))
```

## Integration with other tools (Adapters)

`dj.recorder` keeps a strict `zero dependencies` footprint. However, it ships with optional adapter namespaces that allow it to act as the durable backend for other Clojure tools. 

For example, you can use it to durably memoize crash-safe LLM pipelines with `dj.concurrency`:
```clojure
(require '[dj.concurrency :as c]
         '[dj.recorder.adapter.concurrency :as adapter])

;; If your script crashes mid-pipeline, it resumes instantly from this log
(def sup (c/create-supervisor {:store (adapter/recorder-store db [:results "run-42"])}))
```

## Developing on this repo

```bash
nix develop                       # enter dev shell (JDK + Clojure CLI + babashka)
clojure -M:nrepl                  # start an nREPL server
clojure -M:test                   # run the core suite
```
*(Adapters live in `src/dj/recorder/adapter/` and their tests run via `clojure -M:adapter` to keep the core test suite fast and dependency-free).*

## License
Distributed under the [Eclipse Public License 2.0](LICENSE) (EPL-2.0).
