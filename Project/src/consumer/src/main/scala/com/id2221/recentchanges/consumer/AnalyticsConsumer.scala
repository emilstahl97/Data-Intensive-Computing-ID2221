package com.id2221.recentchanges.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType};
import org.apache.spark.sql.streaming.OutputMode.Complete


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._ 

  val inputStream = spark
       .readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("auto.offset.reset", "latest")
       .option("value.deserializer", "StringDeserializer")
       .option("subscribe", "shops_records")
       .load

  val schema = StructType(
       List(
         StructField("title", StringType, true),
         StructField("user", StringType, true),
         StructField("bot", BooleanType, true),
         StructField("timestamp", TimestampType, true),
         StructField("id", IntegerType, true)
       )
     )

  val initial = inputStream.selectExpr("CAST(value AS STRING)").toDF("value")
  initial.printSchema()

  var aggregation = initial.select(from_json($"value", schema))
  aggregation.printSchema()

  aggregation = initial.select(from_json($"value", schema).alias("tmp")).select("tmp.*")
  aggregation.printSchema()

  val windows = aggregation
       .withWatermark("timestamp", "2 minutes")
       .groupBy(window($"timestamp", "1 minute", "1 minute"), $"title")

  val aggregatedDF = windows.agg(sum("id"), count("*"))

  val dfcount = aggregatedDF
  .writeStream
  .outputMode("complete")
  .option("truncate", false)
  .format("console")
  .start()
  
  dfcount.awaitTermination()

}