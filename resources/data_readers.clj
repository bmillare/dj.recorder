;; Data readers for the Clojure reader so #dj.recorder/* tagged literals in
;; source / read-string round-trip to the patch markers. The log replay path
;; uses clojure.edn/read and must pass dj.recorder.patch/data-readers instead.
{dj.recorder/replace dj.recorder.patch/read-replace
 dj.recorder/dissoc  dj.recorder.patch/read-dissoc
 dj.recorder/splice  dj.recorder.patch/read-splice}
