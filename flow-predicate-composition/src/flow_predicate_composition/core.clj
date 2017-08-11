(ns flow-predicate-composition.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def workflow
  [[:in :identity]
   [:identity :out]])

(defn my-identity [{:keys [n] :as segment}]
  segment)

(def capacity 1000)

(def input-chan (chan capacity))
(def input-buffer (atom {}))

(def output-chan (chan capacity))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :flow-predicate-composition.core/my-identity
    :onyx/type :function
    :onyx/batch-size batch-size
    :parameterized.core/k 42}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def flow-conditions
  [{:flow/from :identity
    :flow/to [:out]
    :flow/short-circuit? true
    :param/one 7
    :param/two 9
    :flow/predicate [:and
                     [:or
                      [:flow-predicate-composition.core/equal-to? :param/one]
                      [:flow-predicate-composition.core/equal-to? :param/two]]
                     :flow-predicate-composition.core/positive-segment?]}
   {:flow/from :identity
    :flow/to [:out]
    :flow/predicate [:not :flow-predicate-composition.core/odd-segment?]}
   {:flow/from :identity
    :flow/to [:out]
    :flow/predicate [:and
                     :flow-predicate-composition.core/div-by-2?
                     :flow-predicate-composition.core/div-by-4?]}])

(defn greater-than-4? [event old {:keys [n]} all-new]
  (> n 4))

(defn equal-to? [event old {:keys [n]} all-new equal-to]
  (= n equal-to))

(defn div-by-2? [event old {:keys [n]} all-new]
  (zero? (mod n 2)))

(defn div-by-4? [event old {:keys [n]} all-new]
  (zero? (mod n 4)))

(defn odd-segment? [event old {:keys [n]} all-new]
  (odd? n))

(defn positive-segment? [event old {:keys [n]} all-new]
  (pos? n))

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
   {:n 9}])

(doseq [segment input-segments]
  (>!! input-chan segment))

(close! input-chan)

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer input-buffer
   :core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :flow-predicate-composition.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :flow-predicate-composition.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def submission
  (onyx.api/submit-job peer-config
                       {:catalog catalog
                        :workflow workflow
                        :lifecycles lifecycles
                        :flow-conditions flow-conditions
                        :task-scheduler :onyx.task-scheduler/balanced}))

(defn -main
  [& args]
  (onyx.api/await-job-completion peer-config (:job-id submission))

  (let [results (take-segments! output-chan 50)]
    (clojure.pprint/pprint results))

  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env))
