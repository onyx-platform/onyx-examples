(ns bounded-retry.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(defn retry-on-failure [f produce-f]
  (try
    (f)
    (catch Exception e
      (produce-f (:segment (ex-data e)))
      [])))

(defn exciting-name [produce-f {:keys [name] :as segment}]
  (println "Receiving " segment)
  (retry-on-failure
   (fn []
     (when (.startsWith name "X")
       (throw (ex-info "Name started with X" {:reason :x-name :segment segment})))
     {:name (str name "!")})
   produce-f))

(def workflow
  [[:in :exciting-name]
   [:exciting-name :out]])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan input-chan})

(defmethod l-ext/inject-temporal-resources :exciting-name
  [_ {:keys [onyx.core/queue onyx.core/ingress-queues onyx.core/task-map] :as context}]
  (let [session (extensions/create-tx-session queue)
        producers (doall (map (partial extensions/create-producer queue session) (vals ingress-queues)))
        n (:retry-on-failure/n task-map)
        f (fn [segment]
            (when (< (or (:failures segment) 0) n)
              (let [retry-segment (assoc segment :failures (inc (or (:failures segment) 0)))]
                (doseq [p producers]
                  (let [context {:onyx.core/results [retry-segment]}
                        compression-context (p-ext/compress-batch context)
                        retry-segment (-> compression-context :onyx.core/compressed first :compressed)]
                    (extensions/produce-message queue p session retry-segment)
                    (extensions/close-resource queue p))))))]
    {:onyx.core/session session
     :error-producers producers
     :produce-f f
     :onyx.core/params [f]}))

(defmethod l-ext/close-temporal-resources :exciting-name
  [_ context]
  (doseq [p (:error-producers context)]
    (extensions/close-resource (:onyx.core/queue context) p))
  {})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan output-chan})

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :exciting-name
    :onyx/fn :bounded-retry.core/exciting-name
    :onyx/type :function
    :onyx/consumption :concurrent
    :retry-on-failure/n 3
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def input-segments
  [{:name "Mike"}
   {:name "Xiu"}
   {:name "Phil"}
   {:name "Julia"}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

(close! input-chan)

(def id (java.util.UUID/randomUUID))

(def scheduler :onyx.job-scheduler/balanced)

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

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/balanced})

(def results (/take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
