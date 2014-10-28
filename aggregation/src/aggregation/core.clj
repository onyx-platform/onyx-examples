(ns aggregation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [clojure.data.fressian :as fressian]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async]
            [onyx.api]))

;;; Word count!

;;; Split a sentence into words, emit a seq of segments
(defn split-sentence [{:keys [sentence]}]
  (map (fn [word] {:word word}) (clojure.string/split sentence #"\s+")))

;;; Use local state to tally up how many of each word we have
;;; Emit the empty vector, we'll emit the local state at the end in one shot
(defn count-words [local-state {:keys [word] :as segment}]
  (swap! local-state (fn [state] (assoc state word (inc (get state word 0)))))
  [])

(def workflow
  [[:input :split-sentence]
   [:split-sentence :count-words]
   [:count-words :output]])

;;; Use core.async for I/O
(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :input
  [_ _] {:core-async/in-chan input-chan})

(defmethod l-ext/inject-lifecycle-resources :output
  [_ _] {:core-async/out-chan output-chan})

;;; Set up local state. Use an atom for keeping track of
;;; how many words we see. Use another atom for idempotent retrying
;;; of sending local state to next task.
(defmethod l-ext/inject-lifecycle-resources :count-words
  [_ _]
  (let [local-state (atom {})]    
    {:onyx.core/params [local-state]
     :aggregation/state local-state
     :aggregation/emitted-state? (atom false)}))

;;; Hook called after each transaction has been committed.
(defmethod l-ext/close-temporal-resources :count-words
  [_ {:keys [onyx.core/queue] :as context}]
  ;;; Only do this when it's the last batch, and we haven't tried this before.
  ;;; This function is called a few times, so it needs to be idempotent.
  (when (and (:onyx.core/tail-batch? context)
             (not @(:aggregation/emitted-state? context)))
    ;;; Grab the local state that we acccrued and compress it with Fressian.
    (let [state @(:aggregation/state context)
          compressed-state (fressian/write state)
          session (:onyx.core/session context)]
      ;;; Use the session and send the local state to all downstream tasks.
      (doseq [queue-name (:onyx.core/egress-queues context)]
        (let [producer (extensions/create-producer queue session queue-name)]
          (extensions/produce-message queue producer session compressed-state)
          (extensions/close-resource queue producer)))
      ;;; Commit the transaction.
      (extensions/commit-tx queue session))
    ;;; We did it! Set the atom so we can idempotently retry this operation.
    (reset! (:aggregation/emitted-state? context) true))
  ;;; All side-effect API hooks must return a map.
  {})

(def batch-size 10)

(def catalog
  [{:onyx/name :input
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :split-sentence
    :onyx/fn :aggregation.core/split-sentence
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :count-words
    :onyx/ident :count-words
    :onyx/fn :aggregation.core/count-words
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}
   
   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

;;; Seriously, my coffee's gone cold. :/
(def input-segments
  [{:sentence "My name is Mike"}
   {:sentence "My coffee's gone cold"}
   {:sentence "Time to get a new cup"}
   {:sentence "Coffee coffee coffee"}
   {:sentence "Om nom nom nom"}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

(close! input-chan)

(def id (java.util.UUID/randomUUID))

(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2186"
   :zookeeper/server? true
   :zookeeper.server/port 2186
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2186"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (onyx.plugin.core-async/take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

