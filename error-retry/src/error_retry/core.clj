(ns error-retry.core
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
      (produce-f {:name (str "Y" (:name (ex-data e)))})
      [])))

(defn exciting-name [produce-f {:keys [name] :as segment}]
  (retry-on-failure
   (fn []
     (when (.startsWith name "X")
       (throw (ex-info "Name started with X" {:reason :x-name :name name})))
     {:name (str name "!")})
   produce-f))

(def workflow
  [[:inc :exciting-name]
   [:exciting-name :out]])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan input-chan})

(defmethod l-ext/inject-temporal-resources :exciting-name
  [_ {:keys [onyx.core/queue onyx.core/ingress-queues] :as context}]
  (let [session (extensions/create-tx-session queue)
        producers (doall (map (partial extensions/create-producer queue session) (vals ingress-queues)))
        f (fn [segment]
            (doseq [p producers]
              (let [context {:onyx.core/results [segment]}
                    compression-context (p-ext/compress-batch context)
                    segment (-> compression-context :onyx.core/compressed first :compressed)]
                (extensions/produce-message queue p session segment)
                (extensions/close-resource queue p))))]
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
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :exciting-name
    :onyx/fn :error-retry.core/exciting-name
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :sequential
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

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/balanced})

(defn take-segments! [ch]
  (loop [x []]
    (let [segment (<!! ch)]
      (let [stack (conj x segment)]
        (if-not (= segment :done)
          (recur stack)
          stack)))))

(def results (take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
