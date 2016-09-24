import spark_base.SparkBase

object StateCount extends SparkBase {
  def main(args: Array[String]) = {
    println(s"ARGS: ${args(0)}")
  }
}
