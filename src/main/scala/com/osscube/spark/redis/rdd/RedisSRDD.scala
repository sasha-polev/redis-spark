package com.osscube.spark.redis.rdd

import java.net.InetAddress
import java.util
import org.apache.spark.{TaskContext, Partition, SparkContext, Logging}
import org.apache.spark.rdd.RDD
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16
import scala.collection.JavaConversions._




class RedisSRDD (  //[K,V]
                  @transient sc: SparkContext,
                  @transient val redisHosts: Array[(String,Int, Int, Int)], //last value is number of partitions per host
                  val namespace: Int = 0,
                  val scanCount: Int = 10000,
                  val keyPattern: String = "*",
                  val valuePattern: String= "*"
                 )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress,endpoint._2)
    //jedis.select(namespace)
    val keys = getKeys(jedis, keyPattern, scanCount, partition)
    logDebug("Keys found: " + keys.mkString(" "))
    //System.out.println("RDD: " + split.index + ", Keys count: " + keys.size()+ ", Mod Max: " + partition.modMax + ", Mod: " + partition.mod)
    keys.flatMap(getVals(jedis, _, keyPattern, scanCount)).iterator
  }

  def getVals(jedis: Jedis, k: String, keyPattern: String, scanCount: Int): Seq[(String, String)]= {
    val params = new ScanParams().`match`(keyPattern) //.count(scanCount)
    val vals = new util.HashSet[String]()
    var scan = jedis.sscan(k,"0",params)
    vals.addAll(scan.getResult)
    while(scan.getStringCursor != "0") {
      scan = jedis.sscan(k, scan.getStringCursor,params)
      vals.addAll(scan.getResult)
    }
    Seq.fill(vals.size){k}.zip(vals)
  }




  def getKeys(jedis: Jedis, keyPattern: String, scanCount: Int, partition: RedisPartition ) = {
    val params = new ScanParams().`match`(keyPattern).count(scanCount)
    //System.out.println("Params k: " + keyPattern + "|" + scanCount)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0",params)
    //System.out.println("Keys count total: " + scan.getResult.size() +", Keys scan cursor: " + scan.getStringCursor )
    val f = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
    //System.out.println("Keys filtered: " + f.length)
    keys.addAll(f)
    while(scan.getStringCursor != "0"){
      scan = jedis.scan(scan.getStringCursor, params)
      val f1  = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
      //System.out.println("Keys filtered: " + f1.length)
      keys.addAll(f1)
      //System.out.println("Keys count total: " + scan.getResult.size() +", Keys scan cursor: " + scan.getStringCursor )
    }
    keys
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

