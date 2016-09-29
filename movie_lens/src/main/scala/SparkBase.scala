package spark_base

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class SparkBase(appName: String = "MovieLens") {
  def sparkConf: SparkConf = 
    new SparkConf().setAppName(appName)

  def sc: SparkContext =
    new SparkContext(sparkConf)
}
