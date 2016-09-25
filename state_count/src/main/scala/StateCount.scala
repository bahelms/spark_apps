import spark_base.SparkBase

object StateCount extends SparkBase {
  def main(args: Array[String]) = {
    val regex: String = """,(?=([^\"]*\"[^\"]*\")*[^\"]*$)"""
    val file = sc.textFile(s"${args(0)}/${args(1)}")

    val header = file.first
    val results = file
      .filter(_ != header)
      .map(_ split regex)
      .map(row => row.map(_.replaceAll("""\s+""", "")))
      .map(_.zipWithIndex)
      .map(row => (row(7)._1.toUpperCase, 1))
      .reduceByKey(_ + _)

    results.saveAsTextFile(s"${args(0)}/scala/spark_apps/state_count/output")
  }
}
