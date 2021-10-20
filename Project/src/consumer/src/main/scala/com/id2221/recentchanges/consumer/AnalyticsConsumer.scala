package com.id2221.recentchanges.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType};
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

  import spark.implicits._ 

  var df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

  df = df.withColumn("value",col("value").cast(StringType)) 

  val schema = new StructType()
        .add("title",StringType)
        .add("user",StringType)
        .add("bot",BooleanType)
        .add("timestamp",StringType)

  var wikiDf = df.select(from_json(col("value"), schema).as("data"))
   .select("data.*")

   // set batch window to 1 minute
   var windowedCountsDF = wikiDf 
    .groupBy(window($"timestamp","4 seconds"), $"title")
	  .agg(avg("timestamp").alias("timestamp"))
    

  // print value to console
  windowedCountsDF.writeStream
    .outputMode("update")
    .format("console")
    .start()
    .awaitTermination()

  spark.streams.awaitAnyTermination()
}
