Redis Spark Connector
=====================

This rather minimalistic connector implements several methods to read data from Redis cluster into Spark RDDs with best data locality.

Use cases:
---------

  * Fast changing data that has to be queried in near-real time
  * Accumulating a stream of updatable records for near-real time access from Spark (writing data is not yet implemented, but can easily be done with plain [Jedis](https://github.com/xetorthio/jedis)

There are three types of structures that can be used depending on use case:
  * Plain Key/Value (can be used in pretty much any scenario)
  * Sets (when multiple values are updated in once, and each value is unique per key. Tis is especially performant when there are many set entries per key)
  * Hashes (special case when key can be logically broken in 2 parts, where first part is common between many records and has meaning. Allows very fast filtering on first part of the key)

Multiple RDD partitions can be created per Redis master node. At the moment reading from slaves is not supported (Jedis does not support it [yet](https://github.com/xetorthio/jedis/issues/790)).

If K or S RDDs are created from the same cluster with similar parameters (i.e same number of partitions per Redis server) they will have same partitioner and can be joined/co-grouped without shuffle.

Example code
------------
```
import com.osscube.spark.redis.rdd._
val rddK1 = sc.redisKInput(("192.168.142.162",7000), 2,keyPattern="@*")
val rddS1 = sc.redisSInput(("192.168.142.162",7000), 2 keyPattern="£*")
rddK1.join(rddS1)....
```

At the moment above code will run without shuffle, but produce 0 results (as keys have to start from different letter and thus never match :-)

It expects the data in the format:

  * For K: `set @key value`
  * For S: `sadd £key value`

Performance
-----------
Above code running on desktop in VM with 8GB of RAM, 8 virtual cores (not dedicated) and 6 instance redis cluster (3 master/3 slave) takes 6 sec. It processes 2 million records that are updated in real time in spark local process with 4Gb of RAM (only 2G assigned to executor). Each key is random 5 letters and value semi-random 512 letters string. It is observed that about 50% of time is used by Spark process GC.
