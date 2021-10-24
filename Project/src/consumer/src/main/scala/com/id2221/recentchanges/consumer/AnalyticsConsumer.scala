package com.id2221.recentchanges.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType};
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

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
  

  val writeNumberOfArticlesToKafka = new Kafka("number-of-articles", "number-of-articles")
  writeNumberOfArticlesToKafka.write(numberOfArticles, "articles")

  val writeChangesPerUserToKafka = new Kafka("users", "users")
  writeChangesPerUserToKafka.write(changesPerUser, "users")

  val writeBotCountToKafka = new Kafka("bots", "bots")
  writeBotCountToKafka.write(botsCount, "bots")
  
  spark.streams.awaitAnyTermination()


class Kafka(topic: String, clientId: String) extends Serializable {

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", clientId)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")
  
  val producer = new KafkaProducer[String, String](props)

  def write(df: DataFrame, topic: String): Unit = {
    val stream = df
    .writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.collect().foreach { row =>
        val record = new ProducerRecord[String, String](topic, row.toString())
        //println("Writing stream " + topic + " to kafka")
        this.producer.send(record)
      }
    }
    .start()
  }
}

/*  // class for writing to HBase database

  class HBase(tableName: String) extends Serializable {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbase")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set("hbase.rpc.timeout", "60000")
    conf.set("hbase.client.scanner.timeout.period", "60000")
    conf.set("hbase.client.operation.timeout", "60000")
    conf.set("hbase.client.write.buffer", "10485760")
    conf.set("hbase.client.retries.number", "3")
    conf.set("hbase.client.pause", "20")
    conf.set("hbase.client.connection.maxidletime", "60000")
    conf.set("hbase.rpc.timeout", "60000")
    conf.set("hbase.client.keyvalue.maxsize", "10485760")
    conf.set("hbase.client.keyvalue.maxsize.checkfreq", "10485760")

    val connection = ConnectionFactory.createConnection(conf)

    val table = connection.getTable(TableName.valueOf(tableName))

    def write(df: DataFrame, tableName: String): Unit = {
      val stream = df
      .writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.collect().foreach { row =>
          val put = new Put(row.getAs[String]("key").getBytes())
          put.addColumn("cf".getBytes(), "title".getBytes(), row.getAs[String]("title").getBytes())
          put.addColumn("cf".getBytes(), "user".getBytes(), row.getAs[String]("user").getBytes())
          put.addColumn("cf".getBytes(), "bot".getBytes(), row.getAs[Boolean]("bot").toString().getBytes())
          put.addColumn("cf".getBytes(), "timestamp".getBytes(), row.getAs[String]("timestamp").getBytes())
          put.addColumn("cf".getBytes(), "id".getBytes(), row.getAs[String]("id").getBytes())
          table.put(put)
        }
      }
      .start()
    }
  
  }
*/
}