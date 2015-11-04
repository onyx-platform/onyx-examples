(defproject datomic-mysql-transfer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [com.datomic/datomic-free "0.9.5173"]
                 [org.onyxplatform/onyx "0.8.0-alpha1"]
                 [org.onyxplatform/onyx-datomic "0.7.0"]
                 [org.onyxplatform/onyx-sql "0.7.0"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [honeysql "0.4.3"]])
