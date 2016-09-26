import spark_base.SparkBase
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import java.io.File

object MovieLens extends SparkBase {
  def main(args: Array[String]) = {
    val file: RDD[String] = sc.textFile(s"${args(0)}/${args(1)}")
    val header: String = file.first

    val results: RDD[(Int, Float)] =
      file
        .filter(_ != header)
        .map(_ split ",")
        .map(row => (row(1).toInt, row(2).toFloat))
        .mapValues((_, 1))
        .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
        .mapValues(v => v._1 / v._2)

    val outputDirectory: String = 
      s"${args(0)}/scala/spark_apps/movie_lens/output"

    FileUtils.deleteDirectory(new File(outputDirectory))
    results.saveAsTextFile(outputDirectory)
  }
}
