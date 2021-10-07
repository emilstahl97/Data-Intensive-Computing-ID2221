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

    val topics = Set("avg")
  
   // read from kafka with spark structured streaming
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      
    // calculate average value of key with spark structured streaming
    val avg = messages.map(x => x._2.toInt).reduce(_+_)
    avg.foreachRDD(rdd => {
      val avg = rdd.reduce(_+_) / rdd.count()
      println(s"Average value of key is $avg")
    })

    // calculate average value of key with spark sql
    val df = spark.read.json(messages.map(x => x._2))
    df.createOrReplaceTempView("avg")
    val avg2 = spark.sql("select avg(value) from avg")
    avg2.show()

    


    ssc.start()
    ssc.awaitTermination()
  }
}