(ns aspect-orientation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [dire.core :as dire]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
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

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core.async/chan input-chan})

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

   {:onyx/name :inc
    :onyx/fn :aspect-orientation.core/my-neg
    :onyx/type :function
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

(def scheduler :onyx.job-scheduler/balanced)

(def env-config
  {:hornetq/mode :vm
   :hornetq.server/type :vm
   :hornetq/server? true
   :zookeeper/address "127.0.0.1:2186"
   :zookeeper/server? true
   :zookeeper.server/port 2186
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2186"
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! output-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)
