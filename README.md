Redis Spark Connector
=====================

This rather minimalistic connector implements several methods to read data from Redis cluster into Spark RDDs with best data locality.

Use cases:
---------

  * Fast changing data that has to be queried in near-real time
  * Accumulating a stream of updatable records for near-real time access from Spark (writing data is not yet implemented, but can easily be done with plain Jedis

There are three types of structures that can be used depending on use case:
  * Plain Key/Value (can be used in pretty much any scenario)
  * Sets (when multiple values are updated in ones, and each value is unique per key. Especially performant when there are many set entries per key)
  * Hashes (special keys when key can be logically broken in 2 parts, where first part is common between many records and has meaning. Allows very fast filtering on first part of the key)
