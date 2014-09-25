(defproject datomic-mysql-transfer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [com.datomic/datomic-free "0.9.4755" :exclusions [commons-codec
                                                                   org.hornetq/hornetq-server]]
                 [com.mdrogalis/onyx "0.3.2"]
                 [com.mdrogalis/onyx-datomic "0.3.2"]
                 [com.mdrogalis/onyx-sql "0.3.2"]
                 [com.mdrogalis/onyx-core-async "0.3.2"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [honeysql "0.4.3"]])

