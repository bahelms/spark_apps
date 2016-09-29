import spark_base.SparkBase
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import java.io.File

object MovieLens extends SparkBase {
  def main(args: Array[String]) = {
    val rootPath: String = args(0)
    val ratingsFile: RDD[String] = 
      sc.textFile(s"${rootPath}/ml-latest/ratings.csv")
    val moviesFile: RDD[String] = 
      sc.textFile(s"{${rootPath}/ml-latest/movies.csv")

    val ratingsHeader: String = ratingsFile.first
    val ratings: RDD[String] = ratingsFile filter (_ != ratingsHeader)

    val moviesHeader: String = moviesFile.first
    val movies: RDD[String] = moviesFile filter (_ != moviesHeader)

    val results: RDD[(String, Float)] = 
      movies.map(_ split ",")
        .map(movie => (movie(0).toInt, movie(1)))
        .join(averageRatingPerMovie(ratings))
        .values

    saveResultsFile(results, s"${rootPath}/scala/spark_apps/movie_lens/output")
  }

  def averageRatingPerMovie(ratings: RDD[String]): RDD[(Int, Float)] = {
    ratings.map(_ split ",")
      .map(row => (row(1).toInt, row(2).toFloat))
      .mapValues((_, 1))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)) // (rating, count)
      .mapValues(v => v._1 / v._2)
  }

  def saveResultsFile(results: RDD[(String, Float)], directory: String) = {
    FileUtils.deleteDirectory(new File(directory))
    results.saveAsTextFile(directory)
  }
}
