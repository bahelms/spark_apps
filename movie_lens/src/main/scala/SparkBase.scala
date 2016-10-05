package spark_base

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class SparkBase(appName: String = "MovieLens") {
  lazy val sparkConf: SparkConf = 
    new SparkConf().setAppName(appName)

  lazy val sc: SparkContext =
    new SparkContext(sparkConf)
}
