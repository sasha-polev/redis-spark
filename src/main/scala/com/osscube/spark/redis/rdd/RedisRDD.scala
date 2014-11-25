package com.osscube.spark.redis.rdd

import java.net.InetAddress
import java.util
import org.apache.spark.{TaskContext, Partition, SparkContext, Logging}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{JedisPool, JedisCluster, HostAndPort, Jedis}
import redis.clients.util.JedisClusterCRC16
import scala.collection.JavaConversions._


case class RedisPartition( index: Int,
                           endpoint: (InetAddress, Int), mod: Int, modMax: Int) extends Partition

class RedisRDD (  //[K,V]
                  @transient sc: SparkContext,
                  @transient val redisHosts: Array[(String,Int, Int, Int)] //last value is number of partitions per host
                 )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress,endpoint._2)
    val keys = jedis.keys("*")
    logDebug("Keys found: " + keys.mkString(" "))
    keys.filter(JedisClusterCRC16.getCRC16(_) % partition.modMax == partition.mod).flatMap(k => if(jedis.`type`(k) == "hash") jedis.hgetAll(k) else Seq()).iterator
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

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def redisInput(initialHost: (String,Int), useSlaves: Boolean = false, numPaprtitionsPerNode: Int = 1) =
  {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size){x._2})).map(s =>  (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisRDD(sc,  hosts)
  }

  def redisSInput(initialHost: (String,Int), useSlaves: Boolean = false, numPaprtitionsPerNode: Int = 1) =
  {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size){x._2})).map(s =>  (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisSRDD(sc,  hosts)
  }


  def getSet(jp: JedisPool, useSlaves: Boolean, numPaprtitionsPerNode: Int ) = {
    val s : scala.collection.mutable.Set[(String, Int, Int)] = scala.collection.mutable.Set()
    val resource = jp.getResource()
    val client = resource.getClient()
    var i = 0
    var master = false
    resource.info("replication").split("\n").foreach { repl =>
      if(repl.contains("role"))
      {
        if(repl.contains("master")) {
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
      if(useSlaves && master)
      {
        if(repl.startsWith("slave")) {

          val replSplit = repl.split("=");
          (0 until numPaprtitionsPerNode).foreach {unused =>
            s.add((replSplit(1).replace(",port", ""), replSplit(2).replace(",state", "").toInt, i))
            i+=1
          }
        }
      }
    }
    (s, i-1)
  }


}
