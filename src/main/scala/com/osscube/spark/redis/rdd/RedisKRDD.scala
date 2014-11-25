package com.osscube.spark.redis.rdd

import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._


class RedisKRDD (  //[K,V]
                  @transient sc: SparkContext,
                  @transient val redisHosts: Array[(String,Int, Int, Int)], //last value is number of partitions per host
                  val namespace: Int,
                  val scanCount: Int,
                  val keyPattern: String,
                  val mGetCount: Int
                 )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress,endpoint._2)
    jedis.select(namespace)
    val keys = getKeys(jedis, keyPattern, scanCount, partition)
    keys.grouped(mGetCount).flatMap(getVals(jedis, _))
  }

  def getVals(jedis: Jedis, keys: Set[String]): Seq[(String, String)]= {
      jedis.mget(keys.mkString(" ")).zip(keys)
  }




  def getKeys(jedis: Jedis, keyPattern: String, scanCount: Int, partition: RedisPartition ) = {
    val params = new ScanParams().`match`(keyPattern).count(scanCount)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0",params)
    val f = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
    keys.addAll(f)
    while(scan.getStringCursor != "0"){
      scan = jedis.scan(scan.getStringCursor, params)
      val f1  = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
      keys.addAll(f1)
    }
    keys.toSet
  }



  override protected def getPreferredLocations(split: Partition): Seq[String] =  {
    Seq(split.asInstanceOf[RedisPartition].endpoint._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] =   {
    (0 until redisHosts.size).map(i => {
      new RedisPartition(i,(InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2), redisHosts(i)._3, redisHosts(i)._4).asInstanceOf[Partition]
    }).toArray
  }


}

