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
    .option("subscribe", "wikiflow-topic")
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

  val value = inputStream.selectExpr("CAST(value AS STRING)").toDF("value")
  value.printSchema()

  var valueJson = value.select(from_json($"value", schema))
  valueJson.printSchema()

  valueJson = value.select(from_json($"value", schema).alias("tmp")).select("tmp.*")
  valueJson.printSchema()

  // count the number of edits per user
  //val editsPerUser = valueJson.groupBy("user").count()

  val changesPerUser = valueJson
    .withWatermark("timestamp", "1 minutes")
    .groupBy(window($"timestamp", "1 minute", "1 minute"), $"user", $"bot").count()

  val numberOfArticles = valueJson
    .withWatermark("timestamp", "1 minutes")
    .groupBy(window($"timestamp", "1 minute", "1 minute"), $"window").count()

  val botsCount = valueJson
    .groupBy(window($"timestamp", "1 minute", "1 minute"))
    .agg(
      sum(when($"bot" === true, lit(1))).as("is_bots"),
      sum(when($"bot" === false, lit(1))).as("non_bots")
    )

  val numberOfArticlesToConsole = numberOfArticles
    .writeStream
    .outputMode("complete")
    .option("truncate", false)
    .format("console")
    .start()  

  val changesPerUserToConsole = changesPerUser
    .writeStream
    .outputMode("complete")
    .option("truncate", false)
    .format("console")
    .start()

  val botsCountToConsole = botsCount
    .writeStream
    .outputMode("complete")
    .option("truncate", false)
    .format("console")
    .start()    
  
  spark.streams.awaitAnyTermination()

  
}