(ns datomic-mysql-transfer.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.datomic]
            [onyx.plugin.sql]
            [onyx.api])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

;;;;;;;; First, some set up work for SQL ;;;;;;;;;;;;;

;;; Def some top-level constants to use below

(def db-name "onyx_example")

(def classname "com.mysql.jdbc.Driver")

(def subprotocol "mysql")

(def subname (format "//127.0.0.1:3306/%s" db-name))

(def user "root")

(def password "password")

;;; Concurrency knob that you can tune
(def batch-size 1000)

;;; The table to read out of
(def table :people)

;;; A monotonically increasing integer to partition the table by
(def id-column :id)

;;; JDBC spec to connect to MySQL
(def db-spec
  {:classname classname
   :subprotocol subprotocol
   :subname subname
   :user user
   :password password})

;;; Create a pool of connections for the virtual peers of Onyx to share
(defn pool [spec]
  {:datasource
   (doto (ComboPooledDataSource.)
     (.setDriverClass (:classname spec))
     (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
     (.setUser (:user spec))
     (.setPassword (:password spec))
     (.setMaxIdleTimeExcessConnections (* 30 60))
     (.setMaxIdleTime (* 3 60 60)))})

;;; Create the pool
(def conn-pool (pool db-spec))

;;; Get rid of the database if it exists, makes the example idempotent
(try
  (jdbc/execute! conn-pool [(format "drop database %s" db-name)])
  (catch Exception e
    (.printStackTrace e)))

;;; Recreate the database from scratch
(jdbc/execute! conn-pool [(format "create database %s" db-name)])

(jdbc/execute! conn-pool [(format "use %s" db-name)])

;;; Create the table we'll be reading out of
(jdbc/execute!
 conn-pool
 (vector (jdbc/create-table-ddl
          :people
          [:id :int "PRIMARY KEY AUTO_INCREMENT"]
          [:name "VARCHAR(32)"]
          [:age "INTEGER(4)"])))

;;; Data to insert into the table
(def people
  [{:name "Mike" :age 23}
   {:name "Dorrene" :age 24}
   {:name "Bridget" :age 32}
   {:name "Joe" :age 70}
   {:name "Amanda" :age 25}
   {:name "Steven" :age 30}])

(jdbc/execute! conn-pool [(format "use %s" db-name)])

;;; Insert the table into the "people" SQL table
(doseq [person people]
  (jdbc/insert! conn-pool :people person))


;;;;;;;; First, some set up work for Datomic ;;;;;;;;;;;;;

;;; The URI for the Datomic database that we'll write to
(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

;;; The schema of the database. A user's name and age, semantic
;;; equivalent of the MySQL schema.
(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}
   
   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

;;; Create the DB, connect to it, and transact the schema
(d/create-database db-uri)

(def datomic-conn (d/connect db-uri))

@(d/transact datomic-conn schema)

;;; Create an ID to connect the Coordinator with its peers
(def id (str (java.util.UUID/randomUUID)))

;;; Run the Coordinator with HornetQ and ZooKeeper in memory
(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

;;; Run the peers with HornetQ in memory
(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})

(def workflow {:partition-keys {:read-rows {:prepare-datoms :write-to-datomic}}})

(def catalog
  [{:onyx/name :partition-keys
    :onyx/ident :sql/partition-keys
    :onyx/type :input
    :onyx/medium :sql
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :sql/classname classname
    :sql/subprotocol subprotocol
    :sql/subname subname
    :sql/user user
    :sql/password password
    :sql/table table
    :sql/id id-column
    :sql/rows-per-segment 1000
    :onyx/batch-size batch-size
    :onyx/doc "Partitions a range of primary keys into subranges"}

   {:onyx/name :read-rows
    :onyx/ident :sql/read-rows
    :onyx/fn :onyx.plugin.sql/read-rows
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :sql/classname classname
    :sql/subprotocol subprotocol
    :sql/subname subname
    :sql/user user
    :sql/password password
    :sql/table table
    :sql/id id-column
    :onyx/batch-size batch-size
    :onyx/doc "Reads rows of a SQL table bounded by a key range"}

   {:onyx/name :prepare-datoms
    :onyx/fn :datomic-mysql-transfer.core/prepare-datoms
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Semantically transform the SQL rows to Datomic datoms"}
   
   {:onyx/name :write-to-datomic
    :onyx/ident :datomic/commit-tx
    :onyx/type :output
    :onyx/medium :datomic
    :onyx/consumption :concurrent
    :datomic/uri db-uri
    :onyx/batch-size batch-size
    :onyx/doc "Transacts :datoms to storage"}])

;;; We need to prepare the datoms before we send it to the Datomic plugin.
;;; Set the temp ids and batch the segments into the :datoms key.
(defn prepare-datoms [segment]
  (let [datoms (map (fn [row]
                      {:db/id (d/tempid :com.mdrogalis/people)
                       :user/name (:name row)
                       :user/age (:age row)})
                    (:rows segment))]
    {:datoms datoms}))

;;; Connect to the Coordinator
(def conn (onyx.api/connect :memory coord-opts))

;;; Bring up the working nodes
(def v-peers (onyx.api/start-peers conn 1 peer-opts))

;;; And off we go!
(def job-id (onyx.api/submit-job conn {:catalog catalog :workflow workflow}))

;;; Block until the job is done, then check Datomic
@(onyx.api/await-job-completion conn (str job-id))

;;; Take the value of the database
(def db (d/db datomic-conn))

;;; All the names and ages
(clojure.pprint/pprint 
 (->> db
      (d/q '[:find ?e :where [?e :user/name]])
      (map first)
      (map (partial d/entity db))
      (map (partial into {}))))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

