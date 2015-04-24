(ns multi-output-workflow.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

(defn my-inc [{:keys [n] :as segment}]
  (update-in segment [:n] inc))

(defn my-dec [{:keys [n] :as segment}]
  (update-in segment [:n] dec))

(def workflow
  [[:in :inc]
   [:in :dec]
   [:inc :output-1]
   [:dec :output-2]])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan-1 (chan capacity))

(def output-chan-2 (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core-async/in-chan input-chan})

(defmethod l-ext/inject-lifecycle-resources :output-1
  [_ _] {:core-async/out-chan output-chan-1})

(defmethod l-ext/inject-lifecycle-resources :output-2
  [_ _] {:core-async/out-chan output-chan-2})

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :multi-output-workflow.core/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :dec
    :onyx/fn :multi-output-workflow.core/my-dec
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :output-1
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :output-2
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def input-segments
  [{:n 0}
   {:n 1}
   {:n 2}
   {:n 3}
   {:n 4}
   {:n 5}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

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

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/balanced})

(def results-1 (take-segments! Output-1))

(def results-2 (take-segments! output-chan-2))

(println "Original segments:")
(clojure.pprint/pprint input-segments)

(println)

(println "Output 1:")
(clojure.pprint/pprint results-1)

(println)

(println "Output 2:")
(clojure.pprint/pprint results-2)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
