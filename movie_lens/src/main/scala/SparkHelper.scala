package spark_helper

class SparkHelper(appName: String) {
  val home: String = System.getenv("HOME")

  val spark: SparkSession = 
    SparkSession.builder.appName(appName).getOrCreate()
}
