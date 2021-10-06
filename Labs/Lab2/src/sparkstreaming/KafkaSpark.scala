package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.SparkSession


object KafkaSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaSpark") // local[2] = deploy locally, need 2 threads to read and process data
    val ssc = new StreamingContext(conf, Seconds(10)) // Window of 1 sec (not needed for direct stream??)
    ssc.checkpoint(".")
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("avg")
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topics
    )

    // Extract (key, value) pairs
    val values = kafkaStream.map(x => x._2.split(","))
    val pairs = values.map(x => (x(0), x(1).toDouble))
    pairs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}