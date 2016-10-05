import spark_base.SparkBase
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import java.io.File

object MovieLensRDD extends SparkBase {
  def main(args: Array[String]) = {
    val home: String = System.getenv("HOME")

    val moviesRDD = 
      sc.textFile(s"${home}/ml-latest/movies.csv").map(_ split ",").cache
    val moviesHeader = moviesRDD.first
    val moviesDF = moviesRDD.filter(_ != moviesHeader).toDF





    saveResultsFile(results, s"${home}/scala/spark_apps/movie_lens/output")
  }

  def averageRatingPerMovie(ratings: RDD[String]): RDD[(Int, Float)] = {
  }

  def saveResultsFile(results: RDD[(String, Float)], directory: String) = {
    FileUtils.deleteDirectory(new File(directory))
    results.saveAsTextFile(directory)
  }
}
