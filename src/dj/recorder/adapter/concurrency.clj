(ns dj.recorder.adapter.concurrency
  "Adapter: a dj.concurrency ResultStore backed by a dj.recorder db.

   Lives under `dj.recorder.adapter.*` — the home for opt-in adapters that let
   *other* libraries use dj.recorder as a durable backend by implementing their
   protocols (cf. `ring.adapter.*`). dj.recorder is the provider; dj.concurrency
   is the consumer.

   This namespace is OPT-IN: dj.recorder's own :deps stay empty, and requiring
   `dj.recorder` never loads this file. Only code that wants the adapter needs
   both dj.recorder and dj.concurrency on the classpath and requires this ns.

   Namespaces all entries under path-prefix (e.g. [:results run-id]) so one
   journal can serve many workflows and pruning a run is one dissoc patch."
  (:require [dj.recorder :as r]
            [dj.concurrency.store :as store]))

(defn recorder-store
  "Wraps an open dj.recorder db as a ResultStore.
   path-prefix is a vector, e.g. [:results \"run-42\"].

   Keys are EDN data stored verbatim (dj.concurrency uses the literal key, not a
   hash), so lookups/records are `get-in`/`update-in` with the key as the final
   path segment. record! derefs the per-tx promise: it returns only once the
   patch is durable, giving persist-then-publish semantics upstream."
  [db path-prefix]
  (reify store/ResultStore
    (lookup [_ k]
      (get-in @db (conj path-prefix k)))                       ; instant in-memory read
    (record! [_ k entry]
      @(r/update! db (conj path-prefix k) (constantly entry))  ; block until durable
      nil)
    (evict! [_ k]
      @(r/update! db path-prefix dissoc k)                     ; durable key removal, blocks
      nil)))
