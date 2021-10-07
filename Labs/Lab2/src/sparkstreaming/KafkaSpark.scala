package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaSpark")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("/home/cuong/projects/id2221-labs/temp")

    val spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    val topics = Set("avg")
  
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topics
    )
    val values = messages.map(x => x._2.split(","))
    val pairs = values.map(x => (x(0), x(1).toDouble))
    pairs.print()

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      val sum = value.getOrElse(0.0) + state.getOption().getOrElse(0.0)
      val avg = sum / 2
      state.update(avg)
      (key, avg)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    stateDstream.foreachRDD(rdd => {
      val values = rdd.map(x => x).collect()
      values.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}