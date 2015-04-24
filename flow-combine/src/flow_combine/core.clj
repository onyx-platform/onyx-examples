(ns flow-combine.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def workflow
  [[:in :identity]
   [:identity :out]])

(defn my-identity [{:keys [n] :as segment}]
  segment)

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

   {:onyx/name :identity
    :onyx/ident :flow-combine.core/my-identity
    :onyx/fn :flow-combine.core/my-identity
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
  [{:flow/from :identity
    :flow/to [:out]
    :flow/predicate [:not :flow-combine.core/odd-segment?]}
   {:flow/from :identity
    :flow/to [:out]
    :flow/predicate :flow-combine.core/equal-to-5?}])

(defn equal-to-5? [event {:keys [n]}]
  (= n 5))

(defn odd-segment? [event {:keys [n]}]
  (odd? n))

(def input-segments
  [{:n -3}
   {:n -2}
   {:n -1}
   {:n 0}
   {:n 1}
   {:n 2}
   {:n 3}
   {:n 4}
   {:n 5}
   {:n 6}
   {:n 7}
   {:n 8}
   {:n 9}
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

