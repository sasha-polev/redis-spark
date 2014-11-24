package com.osscube.spark.redis

import org.apache.spark.SparkContext

/**
 * Created by Administrator on 23/11/2014.
 */
package object rdd {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
