(ns multiple-side-effect-dispatch.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

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

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow
  [[:in :inc]
   [:inc :out]])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan input-chan})

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan output-chan})

;; Dispatch by name
(defmethod l-ext/inject-lifecycle-resources :inc
  [_ context]
  (println "inject-lifecycle-resources: Dispatching by name")
  (update-in context [:side-effects] conj :dispatch-by-name))

;; Dispatch by identity
(defmethod l-ext/inject-lifecycle-resources :my/inc
  [_ context]
  (println "inject-lifecycle-resources: Dispatching by identity")
  (update-in context [:side-effects] conj :dispatch-by-ident))

;; Dispatch by type and medium
(defmethod l-ext/inject-lifecycle-resources [:function nil]
  [_ context]
  (println "inject-lifecycle-resources: Dispatching by type and medium")
  (update-in context [:side-effects] conj :dispatch-by-type-and-medium))

;; Dispatch by name to see the value we built up.
(defmethod l-ext/close-lifecycle-resources :inc
  [_ context]
  (println "close-lifecycle-resources: Dispatching by name")
  (println "Conj'ed list: " (:side-effects context)))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/ident :my/inc
    :onyx/fn :multiple-side-effect-dispatch.core/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
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

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
