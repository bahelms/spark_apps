/* Simple app to inspect Auction data */
/* The following import statements are importing SparkContext, all subclasses and SparkConf*/
package exercises
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//Will use max, min - import java.Lang.Math
import java.lang.Math

object AuctionsApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionsApp")
    val sc = new SparkContext(conf)
    /* MAKE SURE THAT PATH TO DATA FILE IS CORRECT */
    val usrhome = System.getenv("HOME")
    val aucFile = usrhome.concat(args(0))
    //set up indexes                

    //build the inputRDD and cache
    val auctionRDD = sc.textFile(aucFile).map(_ split ",").cache

    //total number of bids across all auctions
    val totalBids = auctionRDD.count

    //total number of items (auctions)
    val totalAuctions = auctionRDD.groupBy(_(0)).count

    //RDD containing ordered pairs of auctionid,number
    val bidTotals = auctionRDD map { a => (a(0), 1) } reduceByKey { _ + _ }

    //max, min and avg number of bids
    val maxBids = bidTotals map { _._2 } reduce { Math.max(_, _) }
    val minBids = bidTotals map { _._2 } reduce { Math.min(_, _) }
    val avgBids = totalBids / totalAuctions.toFloat

    //print to console
    println("total bids across all auctions: %s ".format(totalBids))
    println(s"total number of distinct auctions: ${totalAuctions}")
    println(s"Max bids across all auctions: ${maxBids}")
    println(s"Min bids across all auctions: ${minBids}")
    println(s"Avg bids across all auctions: ${avgBids}")
  }
}
