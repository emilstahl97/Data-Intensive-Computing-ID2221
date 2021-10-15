name := "Kafka-AkkaPractice"

version := "0.1"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.typesafe.play" %% "play-json" % "2.8.0"
)