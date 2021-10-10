

package graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming._


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
      (1L, ("Alice", 28)), 
      (2L, ("Bob", 27)), 
      (3L, ("Charlie", 65)), 
      (4L, ("David", 42)), 
      (5L, ("Ed", 55)), 
      (7L, ("Alex", 55)), 
      (6L, ("Fran", 50))
    )

    val edges = Array(
      Edge(4L, 1L, 1),
      Edge(2L, 1L, 2),
      Edge(5L, 2L, 2),
      Edge(7L, 5L, 3),
      Edge(5L, 6L, 3),
      Edge(3L, 6L, 3), 
      Edge(3L, 2L, 4),
      Edge(7L, 6L, 4),
      Edge(2L, 1L, 7),
      Edge(5L, 3L, 8)
    )

    var vertexRDD = spark.sparkContext.parallelize(vertices)
    var edgeRDD = spark.sparkContext.parallelize(edges)
    var graph = Graph(vertexRDD, edgeRDD)
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    println("1. Users that are at least 30 years old: ")
    graph
      .vertices
      .filter { case (id, (name, age)) => age > 30 }
      .collect
      .foreach { case (id, (name, age)) => println(s"$name is $age")}

    println("2. Who likes who: ")
    for (triplet <- graph.triplets.collect){
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    println("3. Someone likes someone else more than 5 times: ")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
    }

    val initialUserGraph = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, user, inDegOpt) => User(user.name, user.age, inDegOpt.getOrElse(0), user.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, user, outDegOpt) => User(user.name, user.age, user.inDeg, outDegOpt.getOrElse(0))
    }

    println("4. Number of people who like each user: ")
    for ((id, property) <- userGraph.vertices.collect) {
      println(s"${property.name} is liked by ${property.inDeg} people.")
    }

    println("5. Names of the users who are liked by the same number of people they like: ")
    userGraph.vertices.filter {
      case (id, user) => user.inDeg == user.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }

    println("6. The oldest follower of each user: ")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
        sendMsg = { triplet => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age) },
        mergeMsg = {(a, b) => if (a._2 > b._2) a else b}
    )

    userGraph.vertices.leftJoin(oldestFollower) {
      (id, user, optOldestFollower) => optOldestFollower match {
        case None => 
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach {
      case (id, str) => println(str)
    }

    spark.stop()
  }
}