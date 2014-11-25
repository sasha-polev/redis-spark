package com.osscube.spark.redis

import java.util
import com.osscube.spark.redis.rdd.{RedisHRDD, RedisKRDD, RedisSRDD}
import org.apache.spark.SparkContext
import redis.clients.jedis.{JedisPool, HostAndPort, JedisCluster}


package object rdd {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
