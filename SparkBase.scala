package spark_base

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

class SparkBase(appName: String = "App") {
  def sparkConf: SparkConf = 
    new SparkConf().setAppName(appName)

  def sc: SparkContext =
    new SparkContext(sparkConf)

  def sqlContext: SQLContext =
    new SQLContext(sc)
}
