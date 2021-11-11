(ns ramper.sieve.flow-receiver)

;;;; NOTICE
;; The protocols and sieve implementations follow closely the ones described by BUBing.

(defprotocol FlowReceiver
  "The FlowReceiver protocol should be implemented by the
  receiver of the new keys that come out of the sieve. This acts
  sort of as a listener for keys that make it through the seive."
  (prepare-to-append [this] "A new flow of keys is ready to be appended to this receiver.")
  (append [this hash key] "A new key is appended")
  (finish-appending [this] "The new flow of keys has finished")
  (no-more-append [this] "No more appends will happen as the underlying sieve was closed.")
  (size [this] "The size of the flow-receiver")
  (dequeue-key [this] "Dequeue a key, return nil if non is available."))
