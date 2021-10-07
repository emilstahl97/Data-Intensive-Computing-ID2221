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
  
    // Read data from Kafka into Spark Streaming
    val initDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "avg")
      .load()
      .select(col("value").cast("string"))

    
    // calculate average for each value with spark streaming
    val avgDf = initDf.groupBy("value").count().withColumn("avg", initDf("count")/initDf("value"))

    // write average to terminal
    val query = avgDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      
          

    ssc.start()
    ssc.awaitTermination()
  }
}