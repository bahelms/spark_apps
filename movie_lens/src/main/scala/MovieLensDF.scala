import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import spark_helper.SparkHelper

object MovieLensDF {
  def main(args: Array[String]) = {
    import spark.implicits._
    val helper: SparkHelper = new SparkHelper("MovieLensDF")
    val spark: SparkSession = helper.spark

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${helper.home}/ml-latest/movies.csv")
      
    val ratingsDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${helper.home}/ml-latest/ratings.csv")

    val moviesWithAvgRating =
      moviesDF.join(ratingsDF, "movieId").groupBy("movieId").agg(avg("rating"))
  }

  def averageRatingPerMovie(ratings: RDD[String]): RDD[(Int, Float)] = {
  }

  def saveResultsFile(results: RDD[(String, Float)], directory: String) = {
    FileUtils.deleteDirectory(new File(directory))
    results.saveAsTextFile(directory)
  }
}
