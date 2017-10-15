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


;; env and peer setup
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

;; db setup for input
(def database-config {
          :description "Derby Localhost in-memory"
          :db-spec {
                    :classname "org.apache.derby.jdbc.EmbeddedDriver"
                    :subprotocol "derby"
                    :subname  "memory:sampleDB;create=true;"
                    ;;:user "none"
                    ;;:password "none"
                    }
          :auto-commit false
          :concurrency :read-only
          :max-rows nil})

(def sql "select
            t1.tablename    ,
            t2.columnname   ,
            t2.columnnumber ,
            t2.columndatatype
          from
          sys.systables as t1 inner join
          sys.syscolumns as t2 on t1.tableid = t2.REFERENCEID
          order by t1.tablename, t2.columnnumber
          fetch first 5 rows only ")


(defn input-table
  ;; TODO Is this lazy? I want to (patiently) be able to stream in many rows...
  ;; https://stackoverflow.com/a/19804765/1638423
  [{:keys [database-config sql row-fn result-set-fn as-arrays?]
    :or {row-fn identity
         result-set-fn vec
         as-arrays? false }
    :as config  }]
  (let [db-spec        (:db-spec database-config)
        db-connection  (j/get-connection db-spec)
        statement (j/prepare-statement db-connection
                                       sql
                                       {:result-type :forward-only
                                        :fetch-size (:fetch-size config)
                                        :max-rows (:max-rows config)
                                        :concurrency :read-only})]
    (j/query db-connection [statement]
             {:as-arrays? as-arrays? ;; do we want an option for this?
                                     ;; Onyx wouldn't understand non-maps so
                                     ;; there will need to be more "middleware".
              :result-set-fn result-set-fn ;; need this to be lazy somehow...
              :row-fn        row-fn})))

(defn inject-in [event lifecycle]
  (prn "test: "  (new java.util.Date)  (type lifecycle) (type event))
  {:seq/rdr "reader?"
   :seq/seq (input-table {:database-config database-config :sql sql})})

(def in-calls {:lifecycle/before-task-start inject-in})

(defn inject-out [event lifecycle] {:core.async/chan output-chan})
(def out-calls {:lifecycle/before-task-start inject-out})

(def lifecycles
  [
   {:lifecycle/task :in  :lifecycle/calls ::in-calls}
   {:lifecycle/task :in  :lifecycle/calls :onyx.plugin.seq/reader-calls}
   {:lifecycle/task :out :lifecycle/calls ::out-calls            }
   {:lifecycle/task :out :lifecycle/calls :onyx.plugin.core-async/writer-calls }])


(defn -main[]
  (let [submission (onyx.api/submit-job peer-config
                                        {:catalog catalog
                                         :workflow workflow
                                         :lifecycles lifecycles
                                         :task-scheduler :onyx.task-scheduler/balanced})
        ]
    (onyx.api/await-job-completion peer-config (:job-id submission))
    (clojure.pprint/pprint (take-segments! output-chan 50)))
;; shut things down
  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))
  (onyx.api/shutdown-peer-group peer-group)
  (onyx.api/shutdown-env env))


;; output from `lein run` :
;;
;; "test: " #inst "2017-10-13T20:10:33.499-00:00" clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap
;; [{:tablename "SYSALIASES",
;;   :columnname "ALIASID",
;;   :columnnumber 1,
;;   :columndatatype
;;   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x7d758929 "CHAR(36) NOT NULL"],
;;   :hash -28695849,
;;   :dot-hash -1617881640,
;;   :md5 "2ae62a9002fc06661815c9c14389d7ba"}
;;  {:tablename "SYSALIASES",
;;   :columnname "ALIAS",
;;   :columnnumber 2,
;;   :columndatatype
;;   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x50ff4c7 "VARCHAR(128) NOT NULL"],
;;   :hash -425882172,
;;   :dot-hash -1165726942,
;;   :md5 "7230cddc031f68dfeec2f09703b6e6ac"}
;;  {:tablename "SYSALIASES",
;;   :columnname "SCHEMAID",
;;   :columnnumber 3,
;;   :columndatatype
;;   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0xb754990 "CHAR(36)"],
;;   :hash 173472465,
;;   :dot-hash -958840684,
;;   :md5 "6b4c2ccda3f5bb63c6c38d91ff536fb5"}
;;  {:tablename "SYSALIASES",
;;   :columnname "JAVACLASSNAME",
;;   :columnnumber 4,
;;   :columndatatype
;;   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x65716cdc "LONG VARCHAR NOT NULL"],
;;   :hash 1483031925,
;;   :dot-hash -2033109780
;; ,
;;   :md5 "e4f3044ff8943a32d090f565ea227ed2"}
;;  {:tablename "SYSALIASES",
;;   :columnname "ALIASTYPE",
;;   :columnnumber 5,
;;   :columndatatype
;;   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x349c8d9a "CHAR(1) NOT NULL"],
;;   :hash 240715369,
;;   :dot-hash -907786358,
;;   :md5 "9b12dee99e2519931092c79ff909919b"}]
