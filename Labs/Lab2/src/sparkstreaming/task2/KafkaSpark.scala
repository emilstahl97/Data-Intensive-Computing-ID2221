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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaSpark")
    val spark = SparkSession.builder.appName("KafkaSpark").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext

    var df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "avg")
    .load()

    // convert the valuer column to string withColumn function
    df = df.withColumn("value",col("value").cast(StringType))

    // select value column and split it by ,
    val value = df.select(split(col("value"), ",").alias("value"))

    // print words to terminal
    //val query = value.writeStream.outputMode("append").format("console").start().awaitTermination()

    // split query into words
    //val words = value.selectExpr("value[0] as word")
    //val number = value.selectExpr("value[1] as number")
    //print words
    //val query2 = words.writeStream.outputMode("append").format("console").start().awaitTermination()
    // print number
    //val query3 = number.writeStream.outputMode("append").format("console").start().awaitTermination()

    // calculate average of words
    val avg = value.selectExpr("value[0] as word", "value[1] as number")
    //print avg
    val query4 = avg.writeStream.outputMode("append").format("console").start().awaitTermination()

    // print df to terminal
    /*
    val query = df.writeStream
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()
    */

    //spark.close
  }
}