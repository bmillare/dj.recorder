(ns dj.durable.protocols
  "Protocols defining the behavior of durable data stores and their storage backends.

  ⚠ REFERENCE CODE — NOT part of the live library. Consumed only by the
  `dj.durable2` prototype (see its ns docstring); the shipping `dj.recorder` path
  does not require this. `DiffTransactor`/`diff-tx!` is the ancestor of the
  current `dj.recorder/tx!`. Kept for design lineage only.")

(defprotocol DiffTransactor
  "Protocol for transacting diffs against a data store."
  (diff-tx! [db diff-fn]))

(defprotocol Compactable
  "Protocol for compacting the underlying append-only log."
  (compact! [db]))

(defprotocol Closeable ;; mimic core.async Channel close logic
  "Protocol for gracefully closing a data store."
  (close! [chan])
  (closed? [chan]))

(defprotocol AppendableStorage
  "Protocol for low-level append-only storage operations."
  (append! [store obj]))

;; - defprotocol move ?
;; - clojure.lang.IDeref


