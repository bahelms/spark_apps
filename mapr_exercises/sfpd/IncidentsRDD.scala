import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/** Uses an RDD with a CSV (sfpd.csv)
 */
object IncidentsRDD {
  def main(args: Array[String]) = {
    val conf: SparkConf = new SparkConf().setAppName("IncidentsRDD")
    val sc: SparkContext = new SparkContext(conf)
    val home: String = System.getenv("HOME")
    val incidents = sc.textFile(s"${home}/${args(0)}").map(_ split ",")

    val totalIncidents: Long = incidents.count
    val totalCategories: Long = incidents.map(_(1)).distinct.count
    val totalDistricts: Long = incidents.map(_(6)).distinct.count
    val districts: Array[String] = incidents.map(_(6)).distinct.collect
    val totalTenderloinIncidents: Long = 
      incidents.filter(_(6) == "TENDERLOIN").count
    val totalRichmondIncidents: Long = 
      incidents.filter(_(6) == "RICHMOND").count
    val categories: Array[String] = incidents.map(_(1)).distinct.collect

    println(s"Total Incidents: ${totalIncidents}")
    println(s"Total Incidents in Tenderloin District: ${totalTenderloinIncidents}")
    println(s"Total Incidents in Richmon District: ${totalRichmondIncidents}")
  }
}
