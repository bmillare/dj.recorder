(ns dj.recorder.patch-property-test
  "Generative lock on the ONE invariant that matters most for a durability
  library: the live in-memory fold and the log-replay fold are the SAME code
  path ([D4]), so a patch applied from memory must agree with the same patch
  serialized (`pr-str`) and read back (`clojure.edn/read`) before applying. A
  divergence here is a silent corruption bug — `@db` would show state a restart
  can't reproduce. The curated worked-examples live in `patch_test.clj`; this
  fuzzes the whole space.

  Spec: agent/ledger/2026-06-27-patch-alpha-sketch.md, alpha decision [D4]."
  (:require [clojure.test :refer [deftest is]]
            [clojure.edn :as edn]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [dj.recorder.patch :as p]))

(defn- reread
  "The disk path: serialize a patch and read it back through the same reader
  config the log replay uses."
  [x]
  (edn/read-string {:readers p/data-readers} (pr-str x)))

;; ---------------------------------------------------------------------------
;; Generators — only values that survive the log's pr-str -> edn/read cycle
;; (patch's [D4] whitelist). Two deliberate narrowings so the equality-based
;; properties test the FOLD invariant rather than textual-format gotchas:
;;   - Floats: FINITE doubles only. ##NaN/##Inf are whitelisted and round-trip
;;     as bytes, but `##NaN` is never `=` to itself — a `clojure.core/=` gotcha,
;;     not a dj.recorder bug.
;;   - Dates: bounded to years ~1970–9999 (millis in [0, 253402300799999]).
;;     `#inst` textual round-trip works only for 4-digit years — `pr` emits a
;;     wider year for extreme Dates (e.g. `#inst "1360559-..."`) that
;;     clojure.edn's reader regex then rejects. A durability note, not a fold
;;     bug: such a Date is refused at append by storage's serialize-and-reparse
;;     backstop (`assert-round-trips!`), so it never reaches the log — it just
;;     isn't caught by `assert-edn!`'s structural whitelist alone.
;; ---------------------------------------------------------------------------

(def ^:private max-inst-ms 253402300799999)   ; 9999-12-31T23:59:59.999Z

(def ^:private gen-scalar
  (gen/one-of
   [gen/small-integer
    gen/large-integer
    (gen/double* {:infinite? false :NaN? false})
    gen/boolean
    gen/string-ascii
    gen/keyword
    gen/keyword-ns
    (gen/return nil)                                  ; identity sub-patch
    (gen/return p/dissoc-kw)                          ; reserved tombstone kw ([D6])
    (gen/fmap #(java.util.Date. (long %)) (gen/choose 0 max-inst-ms))  ; #inst
    gen/uuid]))                                       ; #uuid

(def ^:private gen-key
  (gen/one-of [gen/keyword gen/small-integer gen/string-ascii]))

(def ^:private gen-edn
  "An EDN-safe value (no markers): the whitelist closed under maps / vectors /
  sets / lists."
  (gen/recursive-gen
   (fn [inner]
     (gen/one-of
      [(gen/vector inner 0 4)
       (gen/set gen-scalar)
       (gen/map gen-key inner {:max-elements 4})
       (gen/fmap #(apply list %) (gen/vector inner 0 3))]))  ; realized list ([D5])
   gen-scalar))

(def ^:private gen-hunk
  (gen/hash-map :at (gen/choose 0 6)
                :- (gen/choose 0 3)
                :+ (gen/vector gen-scalar 0 3)))

(def ^:private gen-patch
  "A patch: EDN data plus the three markers, nestable inside patch maps/vectors."
  (gen/recursive-gen
   (fn [inner]
     (gen/one-of
      [(gen/fmap p/read-replace gen-edn)
       (gen/fmap p/read-dissoc (gen/one-of [(gen/set gen-scalar)
                                            (gen/vector gen-scalar 0 4)]))
       (gen/fmap p/read-splice (gen/vector gen-hunk 0 3))
       (gen/map gen-key inner {:max-elements 4})
       (gen/vector inner 0 4)]))
   gen-edn))

(def ^:private N 1000)

(deftest patch-round-trips-through-edn
  ;; Property 1 — the atom of the corruption argument: every whitelisted patch
  ;; equals itself after pr-str -> edn/read. If a single patch value can't
  ;; reproduce itself off disk, no fold built on it can. `assert-edn!` also runs
  ;; on every case, so the generator is proven to stay inside the [D4] whitelist
  ;; (it throws — failing the property — if it ever escapes).
  (let [res (tc/quick-check
             N
             (prop/for-all [pt gen-patch]
               (p/assert-edn! pt)               ; throws if outside the whitelist
               (= pt (reread pt))))]
    (is (:pass? res) (pr-str res))))

(deftest live-fold-equals-replay-fold
  ;; Property 2 — the invariant this file exists for: folding a sequence of
  ;; patches in memory agrees with folding the SAME patches after each has been
  ;; through the log's pr-str -> edn/read. Application may legitimately throw
  ;; (type collisions, out-of-bounds splices); the contract is that BOTH paths
  ;; AGREE — same value on success, or both throw — never a silent divergence
  ;; where memory and a restart disagree.
  (let [fold (fn [baseline patches]
               (try (p/rehydrate baseline patches)
                    (catch Throwable _ ::threw)))
        res  (tc/quick-check
              N
              (prop/for-all [baseline gen-edn
                             patches  (gen/vector gen-patch 0 8)]
                (= (fold baseline patches)
                   (fold baseline (mapv reread patches)))))]
    (is (:pass? res) (pr-str res))))
