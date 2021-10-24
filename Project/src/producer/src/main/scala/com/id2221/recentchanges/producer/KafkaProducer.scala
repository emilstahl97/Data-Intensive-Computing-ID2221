// Program for reading from wikimedia API recentchanges and using Akka to achieve high concurrency.¨
// The GET request is written to a file and then read by the Akka actor system and finally written to Kafka.


package com.id2221.recentchanges.producer

import java.util.Properties

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RestartSource, Source}
import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._

object KafkaProducer extends App with StrictLogging {
  logger.info("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(300)

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._
  import system.dispatcher

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")

  val producer = new KafkaProducer[String, String](props)
  producer.flush()

  logger.info("Kafka producer initialized")

  var msgCounter = 0

  val restartSource = RestartSource.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 10.seconds,
    randomFactor = 0.2
  ) { () =>
    Source.fromFutureSource {
      Http().singleRequest(Get("https://stream.wikimedia.org/v2/stream/recentchange"))
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
    }
  }

  println("Starting to produce messages...")

  restartSource.runForeach(elem => {
    msgCounter += 1

    // Write to Kafka

    val data = new ProducerRecord[String, String]("wiki-recentchanges-topic", elem.data)
    // print data to console
    //println(data)
    producer.send(data)

    if (msgCounter % 100 == 0) {
      logger.info(s"New messages came, total: $msgCounter messages")
    }

  })
}
