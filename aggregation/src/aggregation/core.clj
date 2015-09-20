(ns aggregation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
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

(defn inject-word-count-resources [event lifecycle]
  (let [state (atom {})]
    {:aggregation/state state
     :onyx.core/params [state]}))

(defn close-word-count-resources [event lifecycle]
  (prn "Totals: " @(:aggregation/state event))
  {})

(def count-words-calls
  {:lifecycle/before-task-start inject-word-count-resources
   :lifecycle/after-task-stop close-word-count-resources})

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :split-sentence
    :onyx/fn :aggregation.core/split-sentence
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :count-words
    :onyx/fn :aggregation.core/count-words
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/flux-policy :kill
    :onyx/min-peers 1
    :onyx/batch-size 1000}
   
   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
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

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :aggregation.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :count-words
    :lifecycle/calls :aggregation.core/count-words-calls}
   {:lifecycle/task :out
    :lifecycle/calls :aggregation.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(onyx.plugin.core-async/take-segments! output-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
