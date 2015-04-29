(ns aspect-orientation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [dire.core :as dire]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

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

(def output-chan (chan capacity))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
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
    :onyx/ident :core.async/write-to-chan
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
   :onyx.messaging/impl :core.async
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task inject-in-ch})

(def out-calls
  {:lifecycle/before-task inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.peer.min-peers-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.peer.min-peers-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow :lifecycles lifecycles
                      :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
