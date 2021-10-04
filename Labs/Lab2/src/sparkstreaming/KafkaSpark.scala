package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
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

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "spark-streaming-consumer",
      "zookeeper.connection.timeout.ms" -> "1000")
      
    //val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    
    // Create direct kafka stream with brokers and topics
    val topics = Set("avg")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    // print words
    kafkaStream.foreachRDD(rdd => {
      val words = rdd.map(_._2)
      words.foreach(println)
    })

    // measure the average value for each key in a stateful manner
    /*
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
    }
    val stateDstream = pairs.mapWithState(<FILL IN>)

    ssc.start()
    ssc.awaitTermination()
    */
  }
}
