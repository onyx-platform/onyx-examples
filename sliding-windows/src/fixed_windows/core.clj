(ns sliding-windows.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx.bookkeeper/server? true
   :onyx.bookkeeper/local-quorum? true
   :onyx.bookkeeper/local-quorum-ports [3196 3197 3198]
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def batch-size 10)

(def workflow
  [[:in :identity]
   [:identity :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/uniqueness-key :id
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(def input-segments
  [{:n 0 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:n 1 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:n 2 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:n 3 :event-time #inst "2015-09-13T03:11:00.829-00:00"}
   {:n 4 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:n 5 :event-time #inst "2015-09-13T03:02:00.829-00:00"}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

(close! input-chan)

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
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def windows
  [{:window/id :collect-segments
    :window/task :identity
    :window/type :sliding
    :window/aggregation :onyx.windowing.aggregation/conj
    :window/window-key :event-time
    :window/range [5 :minutes]
    :window/slide [1 :minute]}])

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/refinement :accumulating
    :trigger/on :segment
    :trigger/threshold [5 :elements]
    :trigger/sync ::dump-window!}])

(defn dump-window! [event window-id lower-bound upper-bound state]
  (println (format "Window extent %s, [%s - %s] contents: %s"
                   window-id lower-bound upper-bound state)))

(onyx.api/submit-job
 peer-config
 {:workflow workflow
  :catalog catalog
  :lifecycles lifecycles
  :windows windows
  :triggers triggers
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! output-chan))

;; Sleep until the trigger timer fires.
(Thread/sleep 5000)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
