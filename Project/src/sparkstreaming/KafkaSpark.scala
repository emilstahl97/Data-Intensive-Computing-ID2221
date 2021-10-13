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
    ssc.checkpoint("./checkpoints")

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

    // print messages to terminal
    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(record => println(record._2))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}