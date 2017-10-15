# onyx-seq-sql

An example fetching input segments from a sql database using onyx-seq. 

I believe your first choice should be using [onyx-sql](/onyx-platform/onyx-sql),
however for complex source queries as stated in the documentation you can look
to onyx-seq usage. This is an example of exactly that. Keep in mind with this
sort of setup you likely only want to use a _single peer_ as there won't be any
distribution on the input data.

Something like this should be usable on any input database with a JDBC driver in
your classpath. I chose an in-memory derby database because it has no setup
required. Just change your database connection strings accordingly.
                                                                             
                                                                             
## Usage/Example 

Output from `lein run` :

```` clojure 
 "test: " #inst "2017-10-13T20:10:33.499-00:00" clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap
 [{:tablename "SYSALIASES",
   :columnname "ALIASID",
   :columnnumber 1,
   :columndatatype
   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x7d758929 "CHAR(36) NOT NULL"],
   :hash -28695849,
   :dot-hash -1617881640,
   :md5 "2ae62a9002fc06661815c9c14389d7ba"}
  {:tablename "SYSALIASES",
   :columnname "ALIAS",
   :columnnumber 2,
   :columndatatype
   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x50ff4c7 "VARCHAR(128) NOT NULL"],
   :hash -425882172,
   :dot-hash -1165726942,
   :md5 "7230cddc031f68dfeec2f09703b6e6ac"}
  {:tablename "SYSALIASES",
   :columnname "SCHEMAID",
   :columnnumber 3,
   :columndatatype
   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0xb754990 "CHAR(36)"],
   :hash 173472465,
   :dot-hash -958840684,
   :md5 "6b4c2ccda3f5bb63c6c38d91ff536fb5"}
  {:tablename "SYSALIASES",
   :columnname "JAVACLASSNAME",
   :columnnumber 4,
   :columndatatype
   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x65716cdc "LONG VARCHAR NOT NULL"],
   :hash 1483031925,
   :dot-hash -2033109780 ,
   :md5 "e4f3044ff8943a32d090f565ea227ed2"}
  {:tablename "SYSALIASES",
   :columnname "ALIASTYPE",
   :columnnumber 5,
   :columndatatype
   #object[org.apache.derby.catalog.types.TypeDescriptorImpl 0x349c8d9a "CHAR(1) NOT NULL"],
   :hash 240715369,
   :dot-hash -907786358,
   :md5 "9b12dee99e2519931092c79ff909919b"}]

```` 
