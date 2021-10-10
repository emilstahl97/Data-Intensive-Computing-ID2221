

package graphx

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import java.util.{Date, Properties}

object SocialNetwork {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkGraphx")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("./checkpoints")

    val sparkBuilder = SparkSession.builder.appName("SparkGraphx").getOrCreate()
    sparkBuilder.sparkContext.setLogLevel("ERROR")

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    val sc = spark.sparkContext

    val vertices = Array(
      (1L, ("Alice", "28")), 
      (2L, ("Bob", "27")), 
      (4L, ("David", "42")), 
      (5L, ("Ed", "55")), 
      (7L, ("Alex", "55")), 
      (3L, ("Charlie", "65")), 
      (6L, ("Fran", "50"))
    )
    val vRDD = sc.parallelize(vertices)
    vRDD.take(1) // Array[(Long, (String, String))] = Array((1,(Alice,28)))

    val edges = Array(
      Edge(4L, 1L, "1"),
      Edge(2L, 1L, "2"),
      Edge(5L, 2L, "2"),
      Edge(7L, 5L, "3"),
      Edge(5L, 6L, "3"),
      Edge(3L, 6L, "3"), 
      Edge(3L, 2L, "4"),
      Edge(7L, 6L, "4"),
      Edge(2L, 1L, "7"),
      Edge(5L, 3L, "8")
    )
    val eRDD = sc.parallelize(edges)
    eRDD.take(1)

    spark.stop()
  }
}