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
  `nil` is the identity patch everywhere — a no-op. It is neither a value to
  store nor a delete, so a `state->patch` fn that returns nil (the natural
  \"skip\" reflex) is a safe no-op instead of nuking the root. The three
  intents are distinct and explicit: store a literal nil with
  `#dj.recorder/replace nil`; remove a key with `dissoc`; \"no change\" is nil.
  Two escapes: `#dj.recorder/replace x` overwrites; `dissoc` removes
  (inline keyword tombstone for map keys / vector indices, op form for
  sets and bulk). `#dj.recorder/splice` does ordered positional vector
  edits (insert / delete / range-replace).

  Design decisions are marked [Dn] at their code sites; each rule below, with
  the fuller mechanism at the site and the rationale in the sketch + watson
  RI 31:

  [D1] Inline vector indices are ORIGINAL-relative: within one patch map, edits
       apply first (never shifting the vector), then tombstones highest-index
       first — so a patch always addresses the vector you started with,
       independent of map iteration order. (apply-map)
  [D2] Splice hunks must be non-overlapping and have distinct `:at`; all hunks
       are validated fail-loud before any edit is applied. (validate-hunks!)
  [D3] Dissoc of a record's *basis* key throws (it would silently degrade the
       record to a plain map); extension keys dissoc fine. (dissoc-guarded)
  [D4] Patches must be plain, replayable EDN — `assert-edn!` enforces it and
       the storage append path calls it before writing. Whitelist:
       nil/booleans/numbers/strings/chars/keywords/symbols, java.util.Date
       (#inst) and java.util.UUID (#uuid), the three marker records, and
       collections thereof. All other records/objects are rejected, INCLUDING
       record patches (a record value in *state* is fine to merge into; a
       record used *as a patch* would hit the log as an unreadable tag).
       CAVEAT: the #inst textual round-trip only holds for 4-digit years
       (0001–9999); an extreme java.util.Date is not narrowed here but is
       refused at append by storage's serialize-and-reparse backstop (the tx
       fails loudly, nothing reaches the log).
  [D5] Seq/list patches append into a realized PersistentList — no lazy `concat`
       stack bomb, no laziness in durable state; O(n) per append, so prefer
       vectors for anything that grows. (apply-patch)
  [D6] `:dj.recorder/dissoc` is reserved only as a *map-value* (as a
       set/vector/list element, a map key, or a leaf under #dj.recorder/replace
       it is ordinary data); store it verbatim at a key via
       `#dj.recorder/replace` — `update-in`'s leaf-patch does this. (leaf-patch)
  [D7] A map patch onto nil always builds a map, even with integer keys
       (assoc-in parity) — a path through a missing vector does not conjure a
       vector; seed it first (or #dj.recorder/replace it). Editing a vector at
       index = count appends (assoc parity). (apply-map / update-in)

  Authoring helpers (below) build patches for the two cases hand-nesting is
  tedious: `update-in` (read-modify-write a deep leaf) and `move` (relocate a
  vector element). Call them qualified (`patch/update-in`, `patch/move`)."
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
;; Reserved in map-value position only ([D6]).
(def dissoc-kw :dj.recorder/dissoc)

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
;;
;; NOTE for the storage append path: print-method honors *print-length* /
;; *print-level*. `pr-str` binds only *print-readably* — the writer must pin
;; both vars to nil (e.g. `(binding [*print-length* nil *print-level* nil]
;; (pr-str patch))`) or a REPL-configured truncation silently corrupts the
;; log ([D4]).
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
;; EDN-safety gate ([D4]).
;; ---------------------------------------------------------------------------

(defn- marker? [x]
  (or (instance? Replace x) (instance? Dissoc x) (instance? Splice x)))

(defn- edn-atom?
  "A leaf value that survives pr-str -> clojure.edn/read unchanged (modulo
  #inst reading back as java.util.Date, which IS java.util.Date here)."
  [x]
  (or (nil? x)
      (boolean? x)
      (number? x)        ; longs, doubles (##Inf/##NaN incl.), BigInt/BigDecimal, ratios
      (string? x)
      (char? x)
      (keyword? x)
      (symbol? x)
      (instance? java.util.Date x)   ; prints #inst, edn default reader
      (uuid? x)))                    ; prints #uuid, edn default reader

(defn assert-edn!
  "Throw an ex-info if `p` contains anything that will not round-trip through
  the log's `pr-str` -> `clojure.edn/read` cycle ([D4]) — records other than
  the three markers, arbitrary Java objects, functions, etc. The ex-data
  carries `:dj.recorder/edn-path` (a get-in-style path into the patch; marker
  contents appear under :value / :coll / :hunks) and the offending `:value`,
  so a failed tx promise points at the exact leaf. Returns `p` unchanged, so
  the storage append path can thread it: `(append! (assert-edn! patch))`.

  Deliberately called at APPEND time, not apply time: the live fold could
  digest these values fine — the corruption would only surface on rehydrate,
  the worst possible moment."
  [p]
  (letfn [(walk! [x path]
            (cond
              (edn-atom? x) nil
              (instance? Replace x) (walk! (:value x) (conj path :value))
              (instance? Dissoc x)  (walk! (:coll x)  (conj path :coll))
              (instance? Splice x)  (walk! (:hunks x) (conj path :hunks))
              ;; record check BEFORE map? — records are map?
              (record? x)
              (throw (ex-info "dj.recorder: record in patch — records don't round-trip the EDN log; patch with a plain map (merges into a record value fine) or #dj.recorder/replace a plain-data representation"
                              {:dj.recorder/edn-path path :value x}))
              (map? x) (doseq [[k v] x]
                         (walk! k (conj path k))   ; keys must be EDN too
                         (walk! v (conj path k)))
              (or (vector? x) (set? x) (seq? x))
              (doseq [[i el] (map-indexed vector x)]
                (walk! el (conj path i)))
              :else
              (throw (ex-info "dj.recorder: non-EDN value in patch — it cannot be replayed from the log"
                              {:dj.recorder/edn-path path
                               :value x
                               :type (class x)}))))]
    (walk! p [])
    p))

;; ---------------------------------------------------------------------------
;; The algebra
;; ---------------------------------------------------------------------------

(declare apply-patch)

(defn- vec-remove [v i]
  (into (subvec v 0 i) (subvec v (inc i))))

(defn- dissoc-guarded
  "`dissoc` that refuses to degrade a record to a plain map ([D3]): removing
  a basis key throws; extension keys and plain maps dissoc normally."
  [m k]
  (let [out (dissoc m k)]
    (if (and (record? m) (not (record? out)))
      (throw (ex-info "dj.recorder: dissoc of a record basis key would degrade the record to a map; #dj.recorder/replace the whole value instead"
                      {:key k :value m}))
      out)))

(defn- validate-hunks!
  "Structural + bounds check for splice hunks against the target vector `v`
  ([D2]). Every hunk: integer `:at` >= 0, integer `:-` >= 0, sequential `:+`.
  Sorted by `:at`: extents must not overlap, `:at` values must be distinct,
  and the furthest extent must fit in `v`. Fail-loud with data before any
  edit is applied."
  [v hunks]
  (let [norm   (mapv (fn [h]
                       (let [{:keys [at] rm :- ins :+ :or {rm 0 ins []}} h]
                         (when-not (and (integer? at) (<= 0 at)
                                        (integer? rm) (<= 0 rm)
                                        (sequential? ins))
                           (throw (ex-info "dj.recorder: malformed splice hunk (want {:at nat-int, :- nat-int, :+ sequential})"
                                           {:hunk h :hunks hunks})))
                         {:at at :rm rm}))
                     hunks)
        sorted (sort-by :at norm)]
    (doseq [[{a1 :at r1 :rm :as h1} {a2 :at :as h2}] (partition 2 1 sorted)]
      (when (or (= a1 a2) (> (+ a1 r1) a2))
        (throw (ex-info "dj.recorder: splice hunks overlap or share an :at (order would be ambiguous)"
                        {:hunk-a h1 :hunk-b h2 :hunks hunks}))))
    (when-let [{a :at r :rm} (last sorted)]
      (when (> (+ a r) (count v))
        (throw (ex-info "dj.recorder: splice hunk extends past end of vector"
                        {:at a :- r :count (count v) :hunks hunks}))))))

(defn- apply-splice
  "Apply ordered unix-hunk-style edits to a vector (sketch §1b). Each hunk:
  {:at i :- n :+ [..]} — start index into the ORIGINAL vector, count removed,
  elements inserted. Hunks are validated (non-overlapping, distinct :at, in
  bounds — [D2]) then applied descending by `:at` so lower indices never need
  rebasing."
  [v hunks]
  (when-not (vector? v)
    (throw (ex-info "#dj.recorder/splice requires a vector" {:value v :hunks hunks})))
  (validate-hunks! v hunks)
  (reduce (fn [acc {:keys [at] rm :- ins :+ :or {rm 0 ins []}}]
            (-> (subvec acc 0 at)
                (into ins)
                (into (subvec acc (+ at rm)))))
          v
          (sort-by :at #(compare %2 %1) hunks)))  ; high index first -> prefix stays valid

(defn- apply-map
  "Merge a plain map/record patch `p` into `v` (markers already dispatched in
  `apply-patch`, so `p` here is data). Recurse per key; a value of `dissoc-kw`
  removes that key (map/record) or index (vector); a value of nil leaves the
  key untouched — present or absent (the identity patch; use
  #dj.recorder/replace nil to store a literal nil).

  [D1]: against a vector, ALL keys are original-relative. Two phases: edits
  first (assoc never shifts), then tombstones deferred and applied
  highest-index-first — so multiple deletes, or a delete alongside an edit,
  are deterministic and mean what they said, independent of map iteration
  order. Vector index edits accept 0..count (index = count appends, assoc
  parity); tombstone indices must be 0..count-1."
  [v p]
  (when-not (or (nil? v) (associative? v))
    (throw (ex-info "cannot merge a map-patch into a non-associative value; use #dj.recorder/replace for a shape change"
                    {:value v :patch p})))
  (let [p      (if (record? p) (into {} p) p)  ; reduce-kv on records needs 1.11+; cheap to not care
        edited (reduce-kv
                (fn [acc k pv]
                  (cond
                    ;; nil sub-patch = no change to this key (don't touch it, don't add it).
                    (nil? pv)        acc
                    ;; tombstones deferred to phase 2 ([D1]).
                    (= pv dissoc-kw) acc
                    :else
                    (do (when (and (vector? acc)
                                   (not (and (integer? k) (<= 0 k (count acc)))))
                          (throw (ex-info "dj.recorder: vector patch key must be an index in 0..count (= count appends)"
                                          {:key k :count (count acc) :patch p})))
                        ;; assoc preserves record/vector type
                        (assoc acc k (apply-patch (get acc k) pv)))))
                (or v {})    ; map onto nil always builds a map, int keys incl. ([D7])
                p)
        tombs  (keep (fn [[k pv]] (when (= pv dissoc-kw) k)) p)]
    (if (vector? edited)
      (do (doseq [k tombs]
            (when-not (and (integer? k) (< -1 k (count edited)))
              (throw (ex-info "dj.recorder: inline dissoc index out of bounds for vector"
                              {:key k :count (count edited) :patch p}))))
          (reduce vec-remove edited (sort #(compare %2 %1) tombs)))  ; high first ([D1])
      (if (associative? edited)
        (reduce dissoc-guarded edited tombs)   ; map or record key ([D3])
        (throw (ex-info "inline dissoc target not associative" {:keys (vec tombs) :value edited}))))))

(defn apply-patch
  "Apply patch `p` to current value `v`, returning the new value. The single
  fold used for both live state and log replay. See sketch §3 for the precise
  type-collision rules; incompatible container merges throw (fail loud)."
  [v p]
  (cond
    ;; nil is the identity patch: no change. NOT a value to store and NOT a
    ;; delete — store a literal nil with #dj.recorder/replace nil, remove with
    ;; dissoc. This makes a state->patch fn returning nil (the natural "skip"
    ;; reflex) a safe no-op instead of nuking the root.
    (nil? p)              v
    ;; markers first — they are also map?
    (instance? Replace p) (:value p)
    (instance? Dissoc p)  (let [c (:coll p)]
                            (cond
                              (set? v) (reduce disj v c)
                              (map? v) (reduce dissoc-guarded v c)   ; record-safe ([D3])
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
                    ;; realized PersistentList, not nested lazy concat ([D5]):
                    ;; O(n) per append, but no stack bomb and no laziness in
                    ;; durable state. Prefer vectors for growing collections.
                    (apply list (concat (or v ()) p)))
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
  so they pass through bare (keeps the log clean); collections, records, nil —
  and the reserved tombstone keyword itself, which bare would DELETE the key
  ([D6]) — must be wrapped in `#dj.recorder/replace` to store verbatim."
  [v]
  (if (or (nil? v) (coll? v) (= v dissoc-kw)) (->Replace v) v))

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
  state (race-free). `path` may be empty to update the root. Note the patch
  merges through *maps* on the way down ([D7]) — a path through a missing
  intermediate creates maps, never vectors (assoc-in parity)."
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
