import spark_base.SparkBase
import org.apache.spark.rdd.RDD

object AuctionDataFrames extends SparkBase {
  def main(args: Array[String]) = {
    import sqlContext.implicits._ // For converting RDD into DataFrame implicitly

    // Define schema
    case class Auction(
      aucID: String,
      bid: Float,
      bidTime: Float,
      bidder: String,
      bidRate: Int,
      openBid: Float,
      price: Float,
      itemType: String,
      dtl: Int
    )

    val inputRDD: RDD = sc.textFile(args(0)).map(_ split ",")
    val auctionsRDD: RDD = inputRDD.map(toAuction(_))

    // Convert to DataFrame and register table to allow use of sqlContext
    val auctionsDF = auctionsRDD.toDF
    auctionsDF.registerTempTable("auctionsDF")

    auctionsDF.count // Total bids
    auctionsDF.select("aucID").distinct.count // Total distinct auctions
    auctionsDF.groupBy("aucID").count.collect // Total bids per auction
    auctionsDF  // Max, Min, and Average of bids per Auction
      .groupBy("aucID", "itemType")
      .agg(max("bid"), min("bid"), avg("bid"))
      .show
    auctionsDF.filter("price > 200").count // Number of auctions over 200

    // Stats about the number of bids per auction
    auctionsDF.groupBy("aucID", "itemType").count.describe("count").show
    // OR
    auctionsDF.groupBy("aucID", "itemType").count.agg(max("count"), min("count"), avg("count")).show

    // Use the temp table to execute SQL
    sqlContext.sql("select * from auctionsDF where itemType = 'xbox'").show
  }

  def toAuction(auctionRow: Array[String]): Auction =
    Auction(a(0), a(1).toFloat, a(2).toFloat, a(3), a(4).toInt, 
      a(5).toFloat, a(6).toFloat, a(7), a(8).toInt))
}
