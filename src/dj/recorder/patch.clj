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
    scalar       -> replace (nothing to add)  nil    -> NO CHANGE (identity)
  `nil` is the identity patch everywhere — a no-op (Option 1). It is neither a
  value to store nor a delete, so a `state->patch` fn that returns nil (the
  natural \"skip\" reflex) is a safe no-op instead of nuking the root. The three
  intents are now distinct and explicit: store a literal nil with
  `#dj.recorder/replace nil`; remove a key with `dissoc`; \"no change\" is nil.
  Two escapes: `#dj.recorder/replace x` overwrites; `dissoc` removes
  (inline keyword tombstone for map keys / vector indices, op form for
  sets and bulk). `#dj.recorder/splice` does ordered positional vector
  edits (insert / delete / range-replace).

  Authoring helpers (§ end) build patches for the two cases hand-nesting is
  tedious: `update-in` (read-modify-write a deep leaf) and `move` (relocate a
  vector element). Both shadow nothing you'd want unqualified — call them
  qualified (`patch/update-in`, `patch/move`)."
  (:refer-clojure :exclude [update-in]))

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
  removes that key (map/record) or index (vector, shifts the tail); a value of
  nil leaves the key untouched — present or absent (the identity patch, Option
  1; use #dj.recorder/replace nil to store a literal nil)."
  [v p]
  (when-not (or (nil? v) (associative? v))
    (throw (ex-info "cannot merge a map-patch into a non-associative value; use #dj.recorder/replace for a shape change"
                    {:value v :patch p})))
  (reduce-kv
   (fn [acc k pv]
     (cond
       ;; nil sub-patch = no change to this key (don't touch it, don't add it).
       (nil? pv)        acc
       (= pv dissoc-kw)
       (cond
         (vector? acc)      (vec-remove acc k)   ; k = integer index (shifts tail)
         (associative? acc) (dissoc acc k)       ; map or record key
         :else (throw (ex-info "inline dissoc target not associative" {:key k :value acc})))
       :else (assoc acc k (apply-patch (get acc k) pv)))) ; assoc preserves record/vector type
   (or v {})
   p))

(defn apply-patch
  "Apply patch `p` to current value `v`, returning the new value. The single
  fold used for both live state and log replay. See sketch §3 for the precise
  type-collision rules; incompatible container merges throw (fail loud)."
  [v p]
  (cond
    ;; nil is the identity patch: no change (Option 1). NOT a value to store
    ;; and NOT a delete — store a literal nil with #dj.recorder/replace nil,
    ;; remove with dissoc. This makes a state->patch fn returning nil (the
    ;; natural \"skip\" reflex) a safe no-op instead of nuking the root.
    (nil? p)              v
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
    :else       p))                              ; non-nil scalar -> replace the leaf

(defn rehydrate
  "Replay a sequence of `patches` onto `baseline` — `(reduce apply-patch ...)`."
  [baseline patches]
  (reduce apply-patch baseline patches))

;; ---------------------------------------------------------------------------
;; Authoring helpers — build a patch for the cases hand-nesting is tedious.
;;
;; Both are *patch constructors*: they read the current state and return a
;; plain-data patch (run-now, persist-the-result — no fn in the log). Use them
;; INSIDE the `(fn [s] …)` authoring fn passed to `tx!`, where `s` is the
;; atomic dispatch-thread state, so the read-modify-write stays race-free (the
;; `update!`/`move!` sugar on `dj.recorder` wraps exactly these two calls):
;;
;;   (tx! db (fn [s] (patch/update-in s [:tracks "strobe" :plays] inc)))
;;   (tx! db (fn [s] (patch/move s [:crates "main"] 0 2)))
;; ---------------------------------------------------------------------------

(defn- leaf-patch
  "A patch fragment that REPLACES a leaf with `v` (so these helpers match
  `clojure.core/update-in`/`move` semantics — they overwrite the target, they
  do not additively merge into it). Scalars already replace under the algebra,
  so they pass through bare (keeps the log clean); collections, records, and
  nil must be wrapped in `#dj.recorder/replace` or they'd merge / union /
  concat (or, for nil, no-op) instead of overwriting."
  [v]
  (if (or (nil? v) (coll? v)) (->Replace v) v))

(defn- nest
  "Place a leaf patch fragment `frag` at `path` within an otherwise-empty
  patch. Empty path ⇒ the fragment *is* the whole patch (root edit)."
  [path frag]
  (if (seq path)
    (assoc-in {} (vec path) frag)
    frag))

(defn update-in
  "Read-modify-write a deep leaf. Returns a patch equivalent to
  `(clojure.core/update-in s path f & args)` — it reads `(get-in s path)`,
  applies `f`, and nests the *result* at `path` (overwriting the leaf, so a
  shrinking `f` like `dissoc` is reflected; cf. additive merge, which can't
  remove). The common scalar RMW (e.g. `inc` a counter) yields a clean nested
  literal; collection/nil results are wrapped in `#dj.recorder/replace`.

  Meant for use inside a `tx!` authoring fn so `s` is the dispatch-thread
  state (race-free). `path` may be empty to update the root."
  [s path f & args]
  (nest path (leaf-patch (apply f (get-in s path) args))))

(defn move
  "Relocate the element of a vector from index `from` to index `to`. Returns a
  `#dj.recorder/splice` patch (two original-relative hunks: remove at `from`,
  re-insert at the adjusted point) nested at `path`. `to` is the element's
  final 0-based index in the resulting vector; both indices must be valid for
  the current vector `(get-in s path)`. `from` = `to` is a no-op (returns nil,
  the identity patch). `path` may be empty when the root is the vector."
  [s path from to]
  (let [v (get-in s path)]
    (when-not (vector? v)
      (throw (ex-info "patch/move target is not a vector" {:path path :value v})))
    (let [n (count v)]
      (when-not (and (<= 0 from) (< from n) (<= 0 to) (< to n))
        (throw (ex-info "patch/move index out of bounds"
                        {:from from :to to :count n}))))
    (if (= from to)
      nil                                   ; no-op = identity patch
      (let [el  (nth v from)
            ;; original-relative insert point: removing at `from` shifts
            ;; indices > from left by 1, so a rightward move inserts one past
            ;; the target; a leftward move inserts straight at the target.
            ins (if (>= to from) (inc to) to)]
        (nest path (->Splice [{:at from :- 1} {:at ins :+ [el]}]))))))
