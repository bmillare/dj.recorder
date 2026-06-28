(ns dj.recorder.patch
  "The patch algebra for dj.recorder — deep, *additive* merge with two
  escapes (`#dj.recorder/replace`, `dissoc`) plus the vector splice op.

  Pure; no I/O. The same `apply-patch` folds new state on the write thread
  *and* replays the log on rehydrate (one code path for memory and disk —
  a divergence here would be a corruption bug).

  Spec: agent/ledger/2026-06-27-patch-alpha-sketch.md

  The one principle: an untagged patch is additive.
    map / record -> merge keys (recurse)      set    -> union
    vector       -> concat (append)           list   -> concat (append)
    scalar       -> replace (nothing to add)
  Two escapes: `#dj.recorder/replace x` overwrites; `dissoc` removes
  (inline keyword tombstone for map keys / vector indices, op form for
  sets and bulk). `#dj.recorder/splice` does ordered positional vector
  edits (insert / delete / range-replace).")

;; ---------------------------------------------------------------------------
;; Markers. These are records (so they round-trip through EDN as tagged
;; literals) and are matched in `apply-patch` *before* the generic
;; record/map merge path — records are themselves `map?`, so order matters.
;; ---------------------------------------------------------------------------

(defrecord Replace [value])  ; #dj.recorder/replace x        — overwrite verbatim
(defrecord Dissoc  [coll])   ; #dj.recorder/dissoc coll       — bulk key/member removal
(defrecord Splice  [hunks])  ; #dj.recorder/splice [hunk ...] — ordered vector edits

;; Inline tombstone: a *value* of this keyword in a merge map deletes that
;; key (map) or that index (vector). Same concept as the `Dissoc` op form,
;; spelled ergonomically for "update some keys, delete others" in one map.
(def ^:const dissoc-kw :dj.recorder/dissoc)

;; ---------------------------------------------------------------------------
;; Reader fns (referenced by resources/data_readers.clj for the Clojure
;; reader, and by `data-readers` below for `clojure.edn/read`).
;; ---------------------------------------------------------------------------

(defn read-replace [v] (->Replace v))
(defn read-dissoc  [c] (->Dissoc c))
(defn read-splice  [h] (->Splice h))

;; Readers map for clojure.edn/read — the log replay uses edn/read (not the
;; Clojure reader), so it must be handed these explicitly.
(def data-readers
  {'dj.recorder/replace read-replace
   'dj.recorder/dissoc  read-dissoc
   'dj.recorder/splice  read-splice})

;; ---------------------------------------------------------------------------
;; print-method: round-trip a marker back to its tagged literal so the log
;; is the inspectable, re-readable EDN we advertise.
;; ---------------------------------------------------------------------------

(defmethod print-method Replace [x ^java.io.Writer w]
  (.write w "#dj.recorder/replace ")
  (print-method (:value x) w))

(defmethod print-method Dissoc [x ^java.io.Writer w]
  (.write w "#dj.recorder/dissoc ")
  (print-method (:coll x) w))

(defmethod print-method Splice [x ^java.io.Writer w]
  (.write w "#dj.recorder/splice ")
  (print-method (:hunks x) w))

;; ---------------------------------------------------------------------------
;; The algebra
;; ---------------------------------------------------------------------------

(declare apply-patch)

(defn- vec-remove [v i]
  (into (subvec v 0 i) (subvec v (inc i))))

(defn- apply-splice
  "Apply ordered unix-hunk-style edits to a vector (sketch §1b). Each hunk:
  {:at i :- n :+ [..]} — start index into the ORIGINAL vector, count removed,
  elements inserted. Hunks must not overlap; applied descending by `:at` so
  lower indices never need rebasing."
  [v hunks]
  (when-not (vector? v)
    (throw (ex-info "#dj.recorder/splice requires a vector" {:value v :hunks hunks})))
  (reduce (fn [acc {:keys [at] rm :- ins :+ :or {rm 0 ins []}}]
            (-> (subvec acc 0 at)
                (into ins)
                (into (subvec acc (+ at rm)))))
          v
          (sort-by :at > hunks)))   ; high index first -> prefix stays valid

(defn- apply-map
  "Merge a plain map/record patch `p` into `v` (markers already dispatched in
  `apply-patch`, so `p` here is data). Recurse per key; a value of `dissoc-kw`
  removes that key (map/record) or index (vector, shifts the tail)."
  [v p]
  (when-not (or (nil? v) (associative? v))
    (throw (ex-info "cannot merge a map-patch into a non-associative value; use #dj.recorder/replace for a shape change"
                    {:value v :patch p})))
  (reduce-kv
   (fn [acc k pv]
     (if (= pv dissoc-kw)
       (cond
         (vector? acc)      (vec-remove acc k)   ; k = integer index (shifts tail)
         (associative? acc) (dissoc acc k)       ; map or record key
         :else (throw (ex-info "inline dissoc target not associative" {:key k :value acc})))
       (assoc acc k (apply-patch (get acc k) pv)))) ; assoc preserves record/vector type
   (or v {})
   p))

(defn apply-patch
  "Apply patch `p` to current value `v`, returning the new value. The single
  fold used for both live state and log replay. See sketch §3 for the precise
  type-collision rules; incompatible container merges throw (fail loud)."
  [v p]
  (cond
    ;; markers first — they are also map?
    (instance? Replace p) (:value p)
    (instance? Dissoc p)  (let [c (:coll p)]
                            (cond
                              (set? v) (reduce disj v c)
                              (map? v) (reduce dissoc v c)
                              :else (throw (ex-info "#dj.recorder/dissoc requires a map or set"
                                                    {:value v :coll c}))))
    (instance? Splice p)  (apply-splice v (:hunks p))
    ;; generic additive cases
    (map? p)    (apply-map v p)                  ; records are map? -> merge, type preserved
    (set? p)    (do (when-not (or (nil? v) (set? v))
                      (throw (ex-info "set-patch requires a set or nil" {:value v :patch p})))
                    (into (or v #{}) p))         ; union
    (vector? p) (do (when-not (or (nil? v) (vector? v))
                      (throw (ex-info "vector-patch requires a vector or nil" {:value v :patch p})))
                    (into (or v []) p))          ; concat
    (seq? p)    (do (when-not (or (nil? v) (seq? v))
                      (throw (ex-info "list-patch requires a list/seq or nil" {:value v :patch p})))
                    (concat (or v ()) p))        ; lists/seqs -> append
    :else       p))                              ; scalar -> replace the leaf

(defn rehydrate
  "Replay a sequence of `patches` onto `baseline` — `(reduce apply-patch ...)`."
  [baseline patches]
  (reduce apply-patch baseline patches))
