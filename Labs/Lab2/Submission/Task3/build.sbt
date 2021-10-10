name := "spark_graphx"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.1.2",
  "org.apache.spark" % "spark-sql_2.12" % "3.1.2",
  "org.apache.spark" % "spark-streaming_2.12" % "3.1.2",
  "org.apache.spark"  % "spark-graphx_2.12" % "3.1.2"
)
