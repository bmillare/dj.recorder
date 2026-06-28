(ns dj.recorder.dogfood
  "Alpha item 5 — exercise/dogfood. A small but real app on the public API:
  a DJ crate/library manager. Tracks live in a map keyed by id; crates are
  ORDERED vectors of ids (the splice stress case). We drive the whole public
  surface (open/record!/record!-sync/@db/await/close!) through additive merge,
  set union, vector append, splice, inline + op-form dissoc, #replace, and a
  read-modify-write authoring fn, then exercise durability (close/reopen) and
  torn-tail recovery (surface + discard).

  Run (from the dj.recorder repo root, inside the nix shell):
    clojure -Sdeps '{:paths [\"src\" \"resources\" \"dev\"]}' -M -m dj.recorder.dogfood

  Friction observed while writing this is marked `;; FRICTION:` inline and
  summarized in agent/ledger/2026-06-27-alpha-dogfood-friction.md."
  (:require [dj.recorder :as r]
            [dj.recorder.storage :as storage]
            [clojure.java.io :as io]))

;; A tagged-literal splice reads here because resources/data_readers.clj is on
;; the classpath — so `#dj.recorder/splice [...]` works in source verbatim.

(defn- line [s] (println (str "\n=== " s " ===")))
(defn- show [db] (println "  @db =>" (pr-str @db)))
(defn- dump-log [path]
  (println "  --- on-disk log ---")
  (doseq [l (clojure.string/split-lines (slurp path))]
    (println "  " l))
  (println "  -------------------"))

(defn- fresh-path []
  ;; A path that does NOT yet exist, so `open` exercises the fresh-db branch.
  (let [f (io/file (System/getProperty "java.io.tmpdir")
                   (str "dj-recorder-dogfood-" (System/nanoTime) ".edn"))]
    (.deleteOnExit f)
    (str f)))

(defn -main [& _]
  (let [path (fresh-path)]
    (println "dogfood log path:" path)

    (line "1. open a fresh db (missing file => baseline {})")
    (let [db (r/open path)]
      (show db)                                   ; {}
      (assert (= {} @db))

      (line "2. additive nested merge — add two tracks")
      ;; FRICTION(low): adding a track is a clean literal patch, but every
      ;; write restates the full nesting path {:tracks {<id> {...}}}. Fine
      ;; shallow; gets verbose deep (see step 8, the RI-8 builder question).
      (r/record!-sync db {:tracks {"strobe" {:title "Strobe" :artist "deadmau5"
                                             :bpm 128 :plays 0
                                             :tags #{:progressive :melodic}}}})
      (r/record!-sync db {:tracks {"opus"   {:title "Opus" :artist "Eric Prydz"
                                             :bpm 126 :plays 0
                                             :tags #{:progressive}}}})
      (show db)

      (line "3. additive set union — grow a track's tag set")
      ;; Set patch unions in: no read needed, no clobber. This is the algebra
      ;; at its best — the additive default is exactly what you want.
      (r/record!-sync db {:tracks {"opus" {:tags #{:classic :uplifting}}}})
      (println "  opus tags =>" (pr-str (get-in @db [:tracks "opus" :tags])))
      (assert (= #{:progressive :classic :uplifting}
                 (get-in @db [:tracks "opus" :tags])))

      (line "4. additive vector append — build an ordered crate")
      ;; A crate is an ordered [id ...]; vector patches CONCAT (append).
      (r/record!-sync db {:crates {"mainroom" ["strobe" "opus"]}})
      (show db)
      (assert (= ["strobe" "opus"] (get-in @db [:crates "mainroom"])))

      (line "5. add a third track, then SPLICE it into the middle of the crate")
      (r/record!-sync db {:tracks {"every" {:title "Every Day" :artist "Eric Prydz"
                                            :bpm 126 :plays 0 :tags #{:vocal}}}})
      ;; Insert "every" at index 1 (between strobe and opus): {:at 1 :- 0 :+ [..]}.
      ;; FRICTION(med): the hunk keys `:-`/`:+` are terse-to-cryptic, and the
      ;; splice lives INSIDE the crate map so you must hold "this op targets the
      ;; vector at this path" in your head. Once internalized it reads OK.
      (r/record!-sync db {:crates {"mainroom" #dj.recorder/splice [{:at 1 :- 0 :+ ["every"]}]}})
      (println "  crate =>" (pr-str (get-in @db [:crates "mainroom"])))
      (assert (= ["strobe" "every" "opus"] (get-in @db [:crates "mainroom"])))

      (line "6. SPLICE a move — relocate strobe (idx 0) to the end")
      ;; FRICTION(high): a "move" is two coordinated, original-relative,
      ;; non-overlapping hunks AND you must name the value you're moving
      ;; ("strobe"). There is no move/swap sugar — this is the strongest
      ;; signal that a higher-level helper (or diff->patch) would earn its keep.
      (r/record!-sync db {:crates {"mainroom" #dj.recorder/splice [{:at 0 :- 1}
                                                                   {:at 3 :- 0 :+ ["strobe"]}]}})
      (println "  crate =>" (pr-str (get-in @db [:crates "mainroom"])))
      (assert (= ["every" "opus" "strobe"] (get-in @db [:crates "mainroom"])))

      (line "7. #replace vs additive — reset a tag set wholesale")
      ;; Additive would UNION; to overwrite you need the #replace escape.
      (r/record!-sync db {:tracks {"opus" {:tags #dj.recorder/replace #{:peaktime}}}})
      (println "  opus tags =>" (pr-str (get-in @db [:tracks "opus" :tags])))
      (assert (= #{:peaktime} (get-in @db [:tracks "opus" :tags])))

      (line "8. read-modify-write authoring fn — bump a play counter")
      ;; FRICTION(med, RI-8): the READ is easy (get-in s ...), but you then
      ;; hand-rebuild the full nesting path for the patch. A `d-update-in`
      ;; helper that returns the nested patch map would remove this re-nesting;
      ;; this is the concrete authoring friction RI 8 flagged.
      (r/record!-sync db (fn [s]
                           {:tracks {"strobe" {:plays (inc (get-in s [:tracks "strobe" :plays]))}}}))
      (println "  strobe plays =>" (get-in @db [:tracks "strobe" :plays]))
      (assert (= 1 (get-in @db [:tracks "strobe" :plays])))

      (line "9. FOOTGUN — an authoring fn that returns nil NUKES the state")
      ;; FRICTION(SHARP): returning nil to mean \"skip / no change\" is the
      ;; natural Clojure reflex, but `(apply-patch s nil)` => nil (scalar
      ;; leaf-replace at the ROOT), so the whole db becomes nil and that nil
      ;; is persisted. The no-op idiom is `{}` (empty-map merge), NOT nil.
      ;; Demonstrated here against a THROWAWAY db so we don't harm the main one.
      (let [tmp  (fresh-path)
            tdb  (r/open tmp)]
        (r/record!-sync tdb {:keep 1})
        (try
          (r/record!-sync tdb (fn [_] nil))         ; the reflex "no change"
          (catch Throwable _ nil))
        (println "  after (fn [_] nil), throwaway @db =>" (pr-str @tdb)
                 "  <-- state destroyed, NOT preserved")
        (assert (nil? @tdb))
        ;; The safe no-op: return {} instead.
        (let [tmp2 (fresh-path) tdb2 (r/open tmp2)]
          (r/record!-sync tdb2 {:keep 1})
          (r/record!-sync tdb2 (fn [_] {}))         ; correct no-op
          (println "  after (fn [_] {}),  throwaway @db =>" (pr-str @tdb2) "  <-- preserved")
          (assert (= {:keep 1} @tdb2))
          (r/close! tdb2))
        (r/close! tdb))

      (line "10. fail-loud — bad patch is rejected; the db survives, no halt")
      ;; Merge a map into a scalar (:bpm is 128) — apply-map refuses. record!-sync
      ;; rethrows; state is untouched and the db keeps working.
      (let [before @db]
        (try
          (r/record!-sync db {:tracks {"strobe" {:bpm {:oops "map-into-scalar"}}}})
          (assert false "expected a throw")
          (catch clojure.lang.ExceptionInfo e
            (println "  rejected:" (.getMessage e))))
        (assert (= before @db) "state must be untouched after a rejected tx")
        (assert (nil? (r/halted db)) "a patch error must NOT halt the db")
        ;; still writable afterwards
        (r/record!-sync db {:tracks {"strobe" {:plays 2}}})
        (assert (= 2 (get-in @db [:tracks "strobe" :plays]))))

      (line "11. inline dissoc tombstone — remove a track key")
      (r/record!-sync db {:tracks {"every" :dj.recorder/dissoc}})
      (println "  track ids =>" (pr-str (keys (:tracks @db))))
      (assert (nil? (get-in @db [:tracks "every"])))

      (line "12. #dissoc op form — bulk-remove tags from a set")
      (r/record!-sync db {:tracks {"strobe" {:tags #dj.recorder/dissoc [:melodic]}}})
      (println "  strobe tags =>" (pr-str (get-in @db [:tracks "strobe" :tags])))
      (assert (= #{:progressive} (get-in @db [:tracks "strobe" :tags])))

      (line "13. inspect the on-disk log — every tx is one re-readable EDN line")
      (r/await db)
      (dump-log path)

      (line "14. durability — snapshot, close, reopen, assert identical")
      (let [snapshot @db]
        (r/close! db)
        ;; close! is idempotent: a second call is a no-op, not an error.
        (r/close! db)
        (println "  reopening...")
        (let [db2 (r/open path)]
          (println "  rehydrated @db == pre-close snapshot?" (= snapshot @db2))
          (assert (= snapshot @db2) "rehydrate must reproduce the live state exactly")
          (r/close! db2)))

      (line "15. torn-tail recovery — fabricate a partial trailing record")
      ;; Simulate a crash mid-append: a non-newline-terminated partial line
      ;; appended after the last good record.
      (spit path "{:tracks {\"ghost\" {:title \"half-writ" :append true)
      (println "  appended a torn partial record to the tail")

      (println "\n  15a. :surface (default) — open must RAISE and free the lock")
      (let [raised (try (r/open path) nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (assert raised "surface policy must raise on a torn tail")
        (let [tt (:dj.recorder/torn-tail (ex-data raised))]
          ;; FRICTION(low): inspect via `:text` (UTF-8 string), NOT `:bytes`
          ;; (a byte[] that pr-strs as an opaque #object[[B ...]). The raise
          ;; message says "Inspect :torn-tail" but `:text` is the readable field.
          (println "  raised; torn-tail offset =>" (:offset tt)
                   " text =>" (pr-str (:text tt)))
          (assert (some? tt))))

      (println "\n  15b. :discard — truncate the partial, open clean (lock was freed)")
      (let [db3 (r/open path {:on-torn-tail :discard})]
        (println "  reopened; \"ghost\" present?" (contains? (:tracks @db3) "ghost"))
        (assert (not (contains? (:tracks @db3) "ghost")))
        ;; and it's writable again after the discard
        (r/record!-sync db3 {:tracks {"closer" {:title "Closer" :bpm 124}}})
        (assert (= "Closer" (get-in @db3 [:tracks "closer" :title])))
        (r/close! db3))

      (line "DONE — all assertions passed")
      (println "halt/reopen (I/O failure) is covered by recorder_test.clj; it"
               "needs a forced append failure that's awkward to stage here."))))
