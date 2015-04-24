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
  [[:in :split-sentence]
   [:split-sentence :count-words]
   [:count-words :out]])

;;; Use core.async for I/O
(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core-async/in-chan input-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core-async/out-chan output-chan})

;; This function will be called multiple times at the end
;; since peers are pipelined, so this function has to be idempotent.
;; A delay works great here.
(defn emit-word->count [{:keys [onyx.core/queue] :as event} session local-state]
  (delay
   (let [compressed-state (fressian/write @local-state)]
     ;; Use the session and send the local state to all downstream tasks.
     (doseq [queue-name (vals (:onyx.core/egress-queues event))]
       (let [producer (extensions/create-producer queue session queue-name)]
         (extensions/produce-message queue producer session compressed-state)
         (extensions/close-resource queue producer)))
     ;; Commit the transaction.
     (extensions/commit-tx queue session))))

;; Set up local state. Use an atom for keeping track of
;; how many words we see. The HornetQ session isn't available
;; at this point in the lifecycle, so we create our own and return
;; it. Onyx happily uses our session for transactions instead.
(defmethod l-ext/inject-lifecycle-resources :count-words
  [_ {:keys [onyx.core/queue] :as event}]
  (let [local-state (atom {})
        session (extensions/create-tx-session queue)]
    {:onyx.core/session session
     :onyx.core/params [local-state]
     :aggregation/emit-delay (emit-word->count event session local-state)}))

(defmethod l-ext/close-temporal-resources :count-words
  [_ {:keys [onyx.core/queue] :as event}]
  ;; Only do this when it's the last batch.
  (when (:onyx.core/tail-batch? event)
    (force (:aggregation/emit-delay event)))
  ;; All side-effect API hooks must return a map.
  {})

(def batch-size 10)

(def catalog
  [{:onyx/name :in
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
   
   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

;; Seriously, my coffee's gone cold. :/
(def input-segments
  [{:sentence "My name is Mike"}
   {:sentence "My coffee's gone cold"}
   {:sentence "Time to get a new cup"}
   {:sentence "Coffee coffee coffee"}
   {:sentence "Om nom nom nom"}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

;; The core.async channel to be closed when using batch mode,
;; otherwise an Onyx peer will block indefinitely trying to read.
(close! input-chan)

(def id (java.util.UUID/randomUUID))

(def scheduler :onyx.job-scheduler/balanced)

(def env-config
  {:hornetq/mode :vm
   :hornetq.server/type :vm
   :hornetq/server? true
   :zookeeper/address "127.0.0.1:2186"
   :zookeeper/server? true
   :zookeeper.server/port 2186
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2186"
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/balanced})

(def results (onyx.plugin.core-async/take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

