(ns resume-job.core
  (:require [clojure.core.async :refer [chan <! >! >!! <!! close! timeout go-loop go]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def workflow
  [[:in :add]
   [:add :out]])

(def capacity 1000)

(def input-chan (chan capacity))
(def input-buffer (atom {}))

(def output-chan (chan capacity))

(def batch-size 1)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :add
    :onyx/fn :resume-job.core/my-adder
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2281"
   :zookeeper/server? true
   :zookeeper.server/port 2281
   :onyx/tenancy-id id})

;; This is important, the job snapshot coordinates are only available after a barrier sync
(def barrier-sync 10)

(def peer-config
  {:zookeeper/address "127.0.0.1:2281"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.peer/coordinator-barrier-period-ms barrier-sync
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

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
    :lifecycle/calls :resume-job.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :resume-job.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def job-name (str "resumable-job-" (java.util.UUID/randomUUID)))

;; Resumable jobs are referred to by job-name *not* job-id.
;; Remember that jobs are immutable, so once a job-id has
;; completed/been killed it cannot be used again
(def named-job
  {:job-name       job-name
   :catalog        catalog
   :workflow       workflow
   :lifecycles     lifecycles
   :task-scheduler :onyx.task-scheduler/balanced})

(defn my-adder [{:keys [n] :as segment}]
  (assoc segment :n (+ n 13)))

(defn input-segments []
  (for [n (range 100)] {:n n}))

(doseq [segment (input-segments)]
  (>!! input-chan segment))

(def first-job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    named-job)))

(def output-segments-job-1 (onyx.plugin.core-async/take-segments! output-chan 1000))

(println "First job completed" (count output-segments-job-1) "segments.")

(go  
  (println "Waiting for snapshot coordinates to be written to the log")
  (<! (timeout (* 100 barrier-sync)))
  
  (onyx.api/kill-job peer-config first-job-id)

  (def snapshot-coordinates
    (onyx.api/job-snapshot-coordinates peer-config job-name))

  (when-not snapshot-coordinates
    (throw (ex-info "Could not find job snapshot coordinates for" job-name "- might want to wait longer.")))

  (doseq [segment (input-segments)]
    (>!! input-chan segment))

  (def named-job-with-resume-point
    (->> snapshot-coordinates
         (onyx.api/build-resume-point named-job)
         (assoc named-job :resume-point)))

  (def second-job-id
    (:job-id
     (onyx.api/submit-job
      peer-config
      named-job-with-resume-point)))

  (def output-segments-job-2 (onyx.plugin.core-async/take-segments! output-chan 1000))

  (println "Second job completed" (count output-segments-job-2) "segments.")

  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env))
