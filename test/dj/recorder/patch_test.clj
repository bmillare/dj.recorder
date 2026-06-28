(ns dj.recorder.patch-test
  "Verifies the patch algebra against the worked-examples reference in
  agent/ledger/2026-06-27-patch-alpha-sketch.md §2 (lettered to match) plus
  the type-collision / error rules in §3."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [dj.recorder.patch :as p]))

(def ^:private DISS p/dissoc-kw)

(deftest maps
  ;; A. merge: add/update keys, leave others alone
  (is (= {:a 1 :b 3 :c 4} (p/apply-patch {:a 1 :b 2} {:b 3 :c 4})))
  ;; B. nested merge: recurse, bottom out on scalar
  (is (= {:user {:name "Bob" :age 31}}
         (p/apply-patch {:user {:name "Bob" :age 30}} {:user {:age 31}})))
  ;; C. delete a key inline while merging
  (is (= {:a 1} (p/apply-patch {:a 1 :b 2} {:b DISS})))
  ;; D. nil is the IDENTITY patch — "no change" (Option 1). It is neither a
  ;;    delete (use dissoc) nor a value to store (use #replace nil). A nil
  ;;    sub-patch leaves the key untouched, present or absent.
  (is (= {:a 1} (p/apply-patch {:a 1} {:a nil})))                       ; present: unchanged
  (is (= {} (p/apply-patch {} {:a nil})))                              ; absent: NOT added
  (is (= {:a nil} (p/apply-patch {:a 1} {:a (p/read-replace nil)})))   ; store a literal nil
  ;; E. replace a whole subtree (escape) — :age is dropped
  (is (= {:user {:name "Alice"}}
         (p/apply-patch {:user {:name "Bob" :age 30}}
                        {:user (p/read-replace {:name "Alice"})})))
  ;; M. build up from absent/empty (absent treated as empty map)
  (is (= {:a {:b 1}} (p/apply-patch {} {:a {:b 1}})))
  (is (= {:a {:b 1}} (p/apply-patch nil {:a {:b 1}})))
  ;; empty-map patch is a no-op
  (is (= {:a 1} (p/apply-patch {:a 1} {}))))

(deftest sets
  ;; F. union (add members)
  (is (= #{:a :b :c} (p/apply-patch #{:a :b} #{:b :c})))
  ;; G. remove members (op form)
  (is (= #{:a :c} (p/apply-patch #{:a :b :c} (p/read-dissoc #{:b}))))
  ;; union onto nil
  (is (= #{:a} (p/apply-patch nil #{:a}))))

(deftest vectors
  ;; H. concat is the default (append the tail)
  (is (= [1 2 3 4 5] (p/apply-patch [1 2 3] [4 5])))
  ;; I. in-place update via integer-keyed map (recurse into index 0)
  (is (= [{:name "Alice"} {:name "Carol"}]
         (p/apply-patch [{:name "Bob"} {:name "Carol"}] {0 {:name "Alice"}})))
  ;; I2. index removal (shifts the tail) — symmetric with map-key dissoc
  (is (= [10 30] (p/apply-patch [10 20 30] {1 DISS})))
  ;; J. replace the whole vector (escape; shrink/rewrite)
  (is (= [9] (p/apply-patch [1 2 3] (p/read-replace [9]))))
  ;; concat onto nil
  (is (= [1 2] (p/apply-patch nil [1 2]))))

(deftest splices
  ;; I3. pure insert at index 1 (shifts the tail)
  (is (= [10 20 30] (p/apply-patch [10 30] (p/read-splice [{:at 1 :+ [20]}]))))
  ;; I4. replace a range (remove 1 at idx 1, insert two)
  (is (= [10 :a :b 30] (p/apply-patch [10 20 30] (p/read-splice [{:at 1 :- 1 :+ [:a :b]}]))))
  ;; I5. multiple non-overlapping hunks, all original-relative
  (is (= [0 :a 2 :b 3 4]
         (p/apply-patch [0 1 2 3 4]
                        (p/read-splice [{:at 1 :- 1 :+ [:a]} {:at 3 :+ [:b]}]))))
  ;; pure delete
  (is (= [10 30] (p/apply-patch [10 20 30] (p/read-splice [{:at 1 :- 1}]))))
  ;; insert at the end
  (is (= [1 2 3] (p/apply-patch [1 2] (p/read-splice [{:at 2 :+ [3]}]))))
  ;; hunks given out of order still apply correctly (sorted descending)
  (is (= [0 :a 2 :b 3 4]
         (p/apply-patch [0 1 2 3 4]
                        (p/read-splice [{:at 3 :+ [:b]} {:at 1 :- 1 :+ [:a]}])))))

(deftest lists-seqs
  ;; H2. a seq serializes to a list; list patch concats (same rule as vectors)
  (is (= '(1 2 3 4 5) (p/apply-patch '(1 2 3) '(4 5))))
  (is (= '(1 2) (p/apply-patch nil '(1 2)))))

(defrecord Person [name age])

(deftest records
  ;; R. record carries map semantics: untagged patch merges, type preserved
  (let [result (p/apply-patch (->Person "Bob" 30) {:age 31})]
    (is (= (->Person "Bob" 31) result))
    (is (instance? Person result) "record type is preserved through merge")))

(deftest scalars-and-root
  ;; K. a scalar patch overwrites a scalar root
  (is (= 42 (p/apply-patch 41 42)))
  ;; L. changing the root's shape is explicit
  (is (= {:a 1} (p/apply-patch 42 (p/read-replace {:a 1}))))
  ;; nil is the identity patch (Option 1): a leaf is left unchanged...
  (is (= 1 (p/apply-patch 1 nil)))
  ;; ...and a bare nil patch on any root is a no-op — this is the safety net for
  ;; a state->patch fn that returns nil (the natural "skip" reflex).
  (is (= {:a 1} (p/apply-patch {:a 1} nil))))

(deftest collision-errors
  (testing "map-patch onto a scalar throws (shape change must be #replace)"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch 42 {:a 1}))))
  (testing "set-patch onto a non-set throws"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch {:a 1} #{:b}))))
  (testing "vector-patch onto a non-vector throws"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch {:a 1} [1 2]))))
  (testing "list-patch onto a non-list throws"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch {:a 1} '(1 2)))))
  (testing "op-form dissoc onto a non-map/set throws"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch 42 (p/read-dissoc [:a])))))
  (testing "splice onto a non-vector throws"
    (is (thrown? clojure.lang.ExceptionInfo (p/apply-patch {:a 1} (p/read-splice [{:at 0 :+ [1]}]))))))

(deftest rehydrate-folds-the-log
  ;; The replay example from sketch §2
  (is (= {:user {:age 31} :settings {:theme "dark"}}
         (p/rehydrate {}
                      [{:user {:name "Bob"}}
                       {:user {:age 30}}
                       {:user {:age 31}}
                       {:settings {:theme "dark"}}
                       {:user {:name DISS}}]))))

(deftest round-trip-through-edn
  ;; Markers must print as their tagged literals and re-read identically —
  ;; this is the actual log persist/replay path (pr-str + edn/read).
  (doseq [x [(p/read-replace {:name "Alice"})
             (p/read-replace [9])
             (p/read-dissoc #{:b :c})
             (p/read-splice [{:at 1 :- 1 :+ [:a :b]} {:at 3 :+ [:b]}])
             ;; nested inside a plain patch map
             {:user (p/read-replace {:x 1}) :tags (p/read-dissoc #{:old})}]]
    (is (= x (edn/read-string {:readers p/data-readers} (pr-str x)))
        (str "round-trip: " (pr-str x)))))

(deftest end-to-end-edn-log-replay
  ;; Simulate the full disk path: print each patch, read it back, fold it.
  (let [patches [{:user {:name "Bob"}}
                 {:user {:age 30}}
                 {:user (p/read-replace {:name "Alice"})}
                 {:tags #{:a :b}}
                 {:tags (p/read-dissoc #{:a})}
                 {:items [1 2 3]}
                 {:items (p/read-splice [{:at 1 :+ [:x]}])}]
        lines    (map pr-str patches)
        reread   (map #(edn/read-string {:readers p/data-readers} %) lines)]
    (is (= {:user {:name "Alice"}
            :tags #{:b}
            :items [1 :x 2 3]}
           (p/rehydrate {} reread)))))
