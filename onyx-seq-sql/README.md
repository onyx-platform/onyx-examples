# onyx-seq-sql

An example fetching input segments from a sql database using onyx-seq.

I believe (as documented) your first choice should be
using [onyx-sql](/onyx-platform/onyx-sql) as it offers many additional choices
for parallel reads/etc.

You may not be able to utilize onyx-sql for reasons such as:
1. complex source queries, correlated/uncorrelated sub-queries, etc. 
2. You deliberately decide to push work such as a join to the source database
   (For instance due to bandwidth transferring two tables to the cloud from an
   on-prem data warehouse).
3. An input table that doesn't have a numeric primary key/unique index or
   distributed reads or in the case of mysql an input table that does not have a
   numeric primary key or a UUID column.

Keep in mind with this sort of usage you likely want to use a _single peer_ as
there won't be any distribution on the input data.

In the `input-table` function is returning a vector of maps which is then
tied to the `:seq/seq` part of the `ingect-in` function/life-cycle entries. I am
not 100% sure this is a lazy sequence however, I've explored this in a stack
overflow question [here](https://stackoverflow.com/a/19804765/1638423).

## Usage/Example

Something like this should be usable on any input database with a JDBC driver in
your classpath. I chose an in-memory derby database because it has no setup
required. Just change your database connection strings accordingly.

The input query is just from the database metadata; Apart from that the derby
database would be empty as it's just an in-memory one for this session.

Probably do a `lein deps`first to make sure you have all the dependencies you need. 

Then you can do `lein run`, and expect output such as:

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
