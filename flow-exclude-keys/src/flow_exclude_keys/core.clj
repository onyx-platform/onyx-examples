(ns flow-exclude-keys.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def workflow
  [[:in :add]
   [:add :out]])

(defn my-adder [{:keys [n] :as segment}]
  (assoc segment
    :n (+ n 5)
    :key-to-exclude "value"
    :key-to-keep "other value"))

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core-async/in-chan input-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core-async/out-chan output-chan})

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :add
    :onyx/ident :flow-exclude-keys.core/my-adder
    :onyx/fn :flow-exclude-keys.core/my-adder
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :parameterized.core/k 42}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def flow-conditions
  [{:flow/from :add
    :flow/to [:out]
    :flow/predicate :flow-exclude-keys.core/always-true
    :flow/exclude-keys [:key-to-exclude]}])

(def always-true (constantly true))

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
  {:hornetq/mode :vm
   :hornetq.server/type :vm
   :hornetq/server? true
   :zookeeper/address "127.0.0.1:2186"
   :zookeeper/server? true
   :zookeeper.server/port 2186
   :onyx/id id})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2186"
   :onyx/id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced})

(def env (onyx.api/start-env env-config))

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :flow-conditions flow-conditions
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

