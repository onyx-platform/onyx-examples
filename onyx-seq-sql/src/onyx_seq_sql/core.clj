(ns onyx-seq-sql.core
    (:require [clojure.core.async :refer [chan >!! <!! close!]]
              [onyx.plugin.core-async :refer [take-segments!]]
              [clojure.java.jdbc :as j]
              [onyx.plugin.seq]
              [onyx.api]))

(defn ^String md5 [s]
  "Example of returning md5 hash of strings."
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes s))]
    (format "%032x" (BigInteger. 1 raw))))

(defn my-func [segment]
  "just hashes a segment, and adds that as a key."
  (conj segment
        {:hash     (hash segment      )}
        {:dot-hash (.hashCode segment )}
        {:md5      (md5 (str segment  ))}))

(def workflow
  [[:in :my-func]
   [:my-func :out]])

(def capacity 1000)
(def input-chan (chan capacity))
(def input-buffer (atom {}))
(def output-chan (chan capacity))
(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.seq/input
    :onyx/type :input
    :onyx/medium :seq
    :seq/checkpoint? false
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a seq"}
   {:onyx/name :my-func
    :onyx/fn ::my-func
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Some func i'm playing with."}
   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}]
  )


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
(def dbc {
          :description "Derby Localhost"
          :db-spec {
                    :classname "org.apache.derby.jdbc.EmbeddedDriver"
                    :subprotocol "derby"
                    :subname  "memory:sampleDB;create=true;"
                    ;;:user "none"
                    ;;:password "none"
                    }
          :field-delim "\u0001"
          :row-delim "\n"
          :auto-commit false
          :concurrency :read-only
          :max-rows nil
          })

(def sql "select
            t1.tablename    ,
            t2.columnname   ,
            t2.columnnumber ,
            t2.columndatatype
          from
          sys.systables as t1 inner join
          sys.syscolumns as t2 on t1.tableid = t2.REFERENCEID
          order by t1.tablename, t2.columnnumber
          fetch first 10 rows only ")


(defn input-table ^clojure.lang.PersistentStructMap
  [{:keys [dbc sql row-fn result-set-fn as-arrays?]
    :or {row-fn identity
         result-set-fn vec
         as-arrays? false }
    :as config  }]
  (let [
        db-spec        (:db-spec dbc)
        db-connection  (j/get-connection db-spec)
        statement (j/prepare-statement
                   db-connection
                   sql
                   {:result-type :forward-only
                    :fetch-size (:fetch-size config)
                    :max-rows (:max-rows config)
                    :concurrency :read-only})]
    (j/query db-connection [statement]
             {:as-arrays?    as-arrays?    ;; do we want an option for this? How slow are dicts?
              :result-set-fn result-set-fn ;; need this to be lazy?
              :row-fn        row-fn
              })))



(defn inject-in [event lifecycle]
  (prn "test: "  (new java.util.Date)  (type lifecycle) (type event))
  {:seq/rdr "reader?"
   :seq/seq (input-table {:dbc dbc :sql sql})})

(def in-calls {:lifecycle/before-task-start inject-in})

(defn inject-out [event lifecycle] {:core.async/chan output-chan})
(def out-calls {:lifecycle/before-task-start inject-out})

(def lifecycles
  [
   {:lifecycle/task :in  :lifecycle/calls ::in-calls}
   {:lifecycle/task :in  :lifecycle/calls :onyx.plugin.seq/reader-calls}
   {:lifecycle/task :out :lifecycle/calls ::out-calls            }
   {:lifecycle/task :out :lifecycle/calls :onyx.plugin.core-async/writer-calls }])


(defn -main []
  (let [submission (onyx.api/submit-job peer-config
                                        {:catalog catalog
                                         :workflow workflow
                                         :lifecycles lifecycles
                                         :task-scheduler :onyx.task-scheduler/balanced})
        ]
    (onyx.api/await-job-completion peer-config (:job-id submission))
    (clojure.pprint/pprint (take-segments! output-chan 50))
    ))

(-main)
;; output:
;;
;; "test: " #inst "2017-10-13T17:33:08.645-00:00" clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap (-main)
;; []
;; nil

;;joes-onyx.core>

;;(-main)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)
(onyx.api/shutdown-env env)
