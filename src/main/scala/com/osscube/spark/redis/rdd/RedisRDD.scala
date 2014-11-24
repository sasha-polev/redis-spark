package com.osscube.spark.redis.rdd

import java.net.InetAddress
import java.util
import org.apache.spark.{TaskContext, Partition, SparkContext, Logging}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{JedisPool, JedisCluster, HostAndPort, Jedis}
import scala.collection.JavaConversions._


case class RedisPartition( index: Int,
                           endpoint: (InetAddress, Int)) extends Partition

class RedisRDD (  //[K,V]
  @transient sc: SparkContext,
  @transient val redisHosts: Array[(String,Int)]
  )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val endpoint = split.asInstanceOf[RedisPartition].endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress,endpoint._2)
    val keys = jedis.keys("*")
    logDebug("Keys found: " + keys.mkString(" "))
    keys.flatMap(k => jedis.hgetAll(k)).iterator
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =  {
     Seq(split.asInstanceOf[RedisPartition].endpoint._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] =   {

    (0 until redisHosts.size).map(i => {
       new RedisPartition(i,(InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2)).asInstanceOf[Partition]
    }).toArray

  }


}
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

    def redisInput(initialHost: (String,Int)) =
    {
      //For now only master nodes
      val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
      val hosts = jc.getClusterNodes.values.filter(jp => jp.getResource.info("replication").split("\n").filter(_.contains("role"))(0).contains("master")).map(jp => {val cl = jp.getResource.getClient; (cl.getHost, cl.getPort)}).toArray
      new RedisRDD(sc,  hosts)
    }
}
