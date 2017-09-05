(ns aspect-orientation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [dire.core :as dire]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api])
  (:gen-class))

(defn my-neg [{:keys [n] :as segment}]
  (assoc segment :n (- n)))

;;; Dire executes pre-conditions before pre-hooks
(dire/with-precondition! #'my-neg
  :evens-only
  (fn [{:keys [n]}] (even? n)))

;;; Dire executes this handler when the specified precondition fails
(dire/with-handler! #'my-neg
  {:precondition :evens-only}
  (fn [e & args]
    (println "[Logger] Rejected segment: " args " (:evens-only)")
    []))

(dire/with-pre-hook! #'my-neg
  (fn [segment]
    (println "[Logger] Accepted segment: " segment)))

(dire/with-post-hook! #'my-neg
  (fn [result]
    (println "[Logger] Emitting segment: " result)))

(def workflow
  [[:in :inc]
   [:inc :out]])

(def capacity 1000)

(def input-chan (chan capacity))
(def in-buffer (atom {}))

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

   {:onyx/name :inc
    :onyx/fn :aspect-orientation.core/my-neg
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
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
   {:n 5}])

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

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan
   :core.async/buffer in-buffer})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :aspect-orientation.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :aspect-orientation.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(defn -main
  [& args]
  (let [submission (onyx.api/submit-job peer-config
                       {:workflow workflow
                        :catalog catalog
                        :lifecycles lifecycles
                        :task-scheduler :onyx.task-scheduler/balanced})]
    (onyx.api/await-job-completion peer-config (:job-id submission)))

  (def results (onyx.plugin.core-async/take-segments! output-chan 50))

  (clojure.pprint/pprint results)

  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env))
