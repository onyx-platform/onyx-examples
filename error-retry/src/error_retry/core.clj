(ns error-retry.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async]
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

(def workflow {:in {:exciting-name :out}})

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core-async/in-chan input-chan})

(defn write-single-message-to-producers [queue session queue-names segment]
  (doseq [producer (map (partial extensions/create-producer queue session) queue-names)]
    (let [context {:onyx.core/results [segment]}
          compression-context (p-ext/compress-batch context)
          segment (-> compression-context :onyx.core/compressed first :compressed)]
      (extensions/produce-message queue producer session segment))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue producer))
  (extensions/close-resource queue session))

(defmethod l-ext/inject-lifecycle-resources :exciting-name
  [_ {:keys [onyx.core/queue onyx.core/ingress-queues] :as context}]
  (let [f (fn [segment]
            (let [session (extensions/create-tx-session queue)]
              (write-single-message-to-producers queue session ingress-queues segment)
              (extensions/close-resource queue session)))]
    {:produce-f f
     :onyx.core/params [f]}))

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

   {:onyx/name :exciting-name
    :onyx/fn :error-retry.core/exciting-name
    :onyx/type :transformer
    :onyx/consumption :concurrent
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

(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2186"
   :zookeeper/server? true
   :zookeeper.server/port 2186
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2186"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

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
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

