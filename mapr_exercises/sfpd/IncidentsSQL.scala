import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.ROW

/** Uses a DataFrame with JSON (sfpd.json)
 */
object IncidentsSQL {
  def main(args: Array[String]) = {
    val conf: SparkConf = new SparkConf().setAppName("IncidentsSQL")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val home: String = System.getenv("HOME")
    val incidents = sqlContext.read.json(s"${home}/${args(0)}")

    incidents.show // first few lines
    incidents.printSchema
    val categories: Array[Row] = 
      incidents.select("Categories").distinct.collect
  }
}
