(ns multi-output-workflow.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
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

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :multi-output-workflow.core/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :dec
    :onyx/fn :multi-output-workflow.core/my-dec
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :output-1
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :output-2
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
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

(defn inject-out-1-ch [event lifecycle]
  {:core.async/chan output-chan-1})

(defn inject-out-2-ch [event lifecycle]
  {:core.async/chan output-chan-2})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-1-calls
  {:lifecycle/before-task-start inject-out-1-ch})

(def out-2-calls
  {:lifecycle/before-task-start inject-out-2-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :multi-output-workflow.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :output-1
    :lifecycle/calls :multi-output-workflow.core/out-1-calls}
   {:lifecycle/task :output-1
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :output-2
    :lifecycle/calls :multi-output-workflow.core/out-2-calls}
   {:lifecycle/task :output-2
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results-1 (take-segments! output-chan-1))

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

(shutdown-agents)