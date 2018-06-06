(ns terminal-reduce-task.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api])
  (:gen-class))

(def input-segments
  [{:n 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:n 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:n 60 :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:n 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:n 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:n 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:n 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:n 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:n 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:n 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:n 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:n 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:n 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:n 3  :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:n 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def input-chan (chan (count input-segments)))
(def input-buffer (atom {}))
(def window-state (atom {}))

(def workflow
  ;; When a reduce task is used as terminal node, an output plugin is no longer
  ;; required.
  [[:in :reducer]])

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer input-buffer
   :core.async/chan input-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def batch-size 20)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :reducer
    :onyx/fn ::triple-n  ;; executed for each segment
    :onyx/type :reduce
    :onyx/max-peers 1
    :onyx/batch-size batch-size}])

(def windows
  [{:window/id :collect-segments
    :window/task :reducer
    :window/type :global
    :window/aggregation [:onyx.windowing.aggregation/min :n]
    :window/window-key :event-time
    :window/init 99}])

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/id :sync
    :trigger/on :onyx.triggers/segment
    :trigger/threshold [5 :elements]
    :trigger/sync ::update-atom!}])

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}])

(defn triple-n [segment]
  (update segment :n (partial * 3)))

(defn update-atom!
  [event window trigger {:keys [lower-bound upper-bound event-type] :as opts} extent-state]
  (when-not (= :job-completed event-type)
    (println "Trigger fired, the atom will be updated!")
    (println (format "Window extent [%s - %s] contents: %s"
                     lower-bound upper-bound extent-state))
    (swap! window-state assoc [lower-bound upper-bound] extent-state)))

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

(defn -main
  [& args]
  (let [submission (onyx.api/submit-job peer-config
                                        {:workflow workflow
                                         :catalog catalog
                                         :lifecycles lifecycles
                                         :windows windows
                                         :triggers triggers
                                         :task-scheduler :onyx.task-scheduler/balanced})]
    (doseq [i input-segments]
      (>!! input-chan i))
    (close! input-chan)

    (onyx.api/await-job-completion peer-config (:job-id submission))

    (println "Final window state: " @window-state)

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))
    (onyx.api/shutdown-peer-group peer-group)
    (onyx.api/shutdown-env env)))
