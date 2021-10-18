package com.id2221.recentchanges.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType};


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Structured consumer")


  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wiki-recentchanges-topic")
    .option("startingOffsets", "earliest")
    .load()

    println("Consumer has started:")
    
    // convert the value column to string withColumn function
    val value = inputStream.withColumn("value", col("value").cast(StringType))


  // please edit the code below
  val transformedStream: DataFrame = value
  
  transformedStream.toJSON.writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
