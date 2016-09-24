import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Subclasses of App may not work correctly
object WordCount {
  def main(args: Array[String]) = {
    val rootPath = "/Users/barretthelms"
    val sc = new SparkContext(new SparkConf().setAppName("WordCount"))
    val fileRDD = 
      sc.textFile(s"${rootPath}/spark-2.0.0-bin-hadoop2.7/README.md")

    val counts = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(s"${rootPath}/scala/spark/word_count/${args(0)}.txt")

    println(counts)
  }
}
