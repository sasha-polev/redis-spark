package com.osscube.spark.redis.rdd

import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._

case class RedisPartition(index: Int,
                          endpoint: (InetAddress, Int), mod: Int, modMax: Int) extends Partition

class RedisPartitioner(val redisHosts: Array[(String, Int, Int, Int)]) extends HashPartitioner(redisHosts.length) {

  override def equals(other: Any): Boolean = other match {
    case h: RedisPartitioner => {
      h.redisHosts.diff(redisHosts).length == 0 && redisHosts.diff(h.redisHosts).length == 0
    }
    case _ =>
      false
  }

}

abstract class BaseRedisRDD (
                             @transient sc: SparkContext,
                             @transient val redisHosts: Array[(String, Int, Int, Int)], //last value is number of partitions per host
                             val namespace: Int,
                             val scanCount: Int = 10000,
                             val keyPattern: String,
                             val makePartitioner: Boolean
                              )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].endpoint._1.getHostName)
  }

  override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new RedisPartitioner(redisHosts)) else None

  override protected def getPartitions: Array[Partition] = {
    (0 until redisHosts.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2), redisHosts(i)._3, redisHosts(i)._4).asInstanceOf[Partition]
    }).toArray
  }

  def getKeys(jedis: Jedis, keyPattern: String, scanCount: Int, partition: RedisPartition) = {
    val params = new ScanParams().`match`(keyPattern).count(scanCount)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0", params)
    val f = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
    keys.addAll(f)
    while (scan.getStringCursor != "0") {
      scan = jedis.scan(scan.getStringCursor, params)
      val f1 = scan.getResult.filter(s => (JedisClusterCRC16.getSlot(s) % (partition.modMax + 1)) == partition.mod)
      keys.addAll(f1)
    }
    keys
  }

}


class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def redisHInput(initialHost: (String, Int),numPaprtitionsPerNode: Int = 1, useSlaves: Boolean = false,  namespace: Int = 0,
                 scanCount: Int = 10000,
                 keyPattern: String = "*",
                 checkForKeyType: Boolean = false) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisHRDD(sc, hosts, namespace,scanCount, keyPattern, checkForKeyType)
  }

  def redisSInput(initialHost: (String, Int),
                  numPaprtitionsPerNode: Int = 1,
                  useSlaves: Boolean = false,
                  namespace: Int = 0,
                  scanCount: Int = 10000,
                  keyPattern: String = "*",
                  makePartitioner: Boolean = true,
                  valuePattern: String = "*"
                   ) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisSRDD(sc, hosts, namespace, scanCount, keyPattern, makePartitioner, valuePattern)
  }

  def getSet(jp: JedisPool, useSlaves: Boolean, numPaprtitionsPerNode: Int) = {
    val s: scala.collection.mutable.Set[(String, Int, Int)] = scala.collection.mutable.Set()
    val resource = jp.getResource()
    val client = resource.getClient()
    var i = 0
    var master = false
    resource.info("replication").split("\n").foreach { repl =>
      if (repl.contains("role")) {
        if (repl.contains("master")) {
          master = true;
          (0 until numPaprtitionsPerNode).foreach { unused =>
            s.add((client.getHost(), client.getPort(), i))
            i += 1
          }
        }
        else {
          i = 0
          master = false
        }
      }
      if (useSlaves && master) {
        if (repl.startsWith("slave")) {

          val replSplit = repl.split("=");
          (0 until numPaprtitionsPerNode).foreach { unused =>
            s.add((replSplit(1).replace(",port", ""), replSplit(2).replace(",state", "").toInt, i))
            i += 1
          }
        }
      }
    }
    (s, i - 1)
  }

  def redisKInput(initialHost: (String, Int),
                  numPaprtitionsPerNode: Int = 1,
                  useSlaves: Boolean = false,
                  namespace: Int = 0,
                  scanCount: Int = 10000,
                  keyPattern: String = "*",
                  makePartitioner: Boolean = true
                   ) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisKRDD(sc, hosts, namespace, scanCount, keyPattern, makePartitioner)
  }


}
