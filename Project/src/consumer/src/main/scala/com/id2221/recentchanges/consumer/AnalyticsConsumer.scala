package com.id2221.recentchanges.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType};
import org.apache.spark.sql.streaming.OutputMode.Complete


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Structured consumer")


  var df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

    println("Started")

    df = df.withColumn("value",col("value").cast(StringType)) 

    // Split by :
    /*
    val value = df.select(
      split(col("value"),",").getItem(13).as("Title")
    )
    */
    /*
    val value = df.select(
      split(col("value"),",").getItem(16).as("User")
    )
    */
    val value = df.select(
      split(col("value"),",").getItem(17).as("isBot")
    )



  // print value to console
  value.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()

  spark.streams.awaitAnyTermination()
}
