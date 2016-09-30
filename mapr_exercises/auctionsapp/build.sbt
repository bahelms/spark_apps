name := "auctionsapp"
version := "1.0.3"
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
excludeFilter in unmanagedSources := "AuctionsDFApp.scala"

