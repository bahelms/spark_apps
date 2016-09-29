package spark_base

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class SparkBase(appName: String = "App") {
  def sparkConf: SparkConf = 
    new SparkConf().setAppName(appName)

  def sc: SparkContext =
    new SparkContext(sparkConf)
}
