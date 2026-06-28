(ns dj.durable.protocols
  "Protocols defining the behavior of durable data stores and their storage backends.")

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


