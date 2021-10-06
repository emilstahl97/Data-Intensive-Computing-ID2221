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
    // make a connection to Kafka and read (key, value) pairs from it
    

    //create a spark context
    val conf = new SparkConf().setAppName("KafkaSpark").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // Added checkpoint t solve the error
    // https://stackoverflow.com/questions/32411252/spark-invalid-checkpoint-directory
    // But doesn't seem to solve it :-/
    ssc.checkpoint("./checkpoints")

    //create spark session
    val spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("avg")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // print kafka stream to terminal 
/*     kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val lines = rdd.map(_._2)
        lines.foreach(println)
      }
    }) */

    val value = kafkaStream.map{case (key, value) => value.split(',')}
    val pairs = value.map(record => (record(1), record(2).toDouble))
    
    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
      val (sum, count) = state.getOption.getOrElse((0.0, 0))
      val updatedSum = value.getOrElse(0.0) + sum
      val updatedCount = count + 1
      state.update((updatedSum, updatedCount))
      (key, updatedSum/updatedCount)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
