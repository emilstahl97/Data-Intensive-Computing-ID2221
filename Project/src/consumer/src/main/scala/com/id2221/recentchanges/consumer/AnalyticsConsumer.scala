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
        .add("timestamp",IntegerType)

  val wikiDf = df.select(from_json(col("value"), schema).as("data"))
   .select("data.*")

  // TODO: Count changed articles per minutes
  /*  Duration windowSize = Duration.ofSeconds(30);
  TimeWindows tumblingWindow = TimeWindows.of(windowSize);

  val countChanges = wikiDf
    .groupBy(col("title"))
    .windowedBy(tumblingWindow)
    .count(); */

  // print value to console
  wikiDf.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()

  spark.streams.awaitAnyTermination()
}
