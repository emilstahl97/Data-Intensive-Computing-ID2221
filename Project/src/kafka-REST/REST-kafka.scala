import java.util.Properties

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.{JsObject, JsValue, Json}
object Producer extends App {

  val url = "https://stream.wikimedia.org/v2/stream/recentchange"
  val httpClient = HttpClientBuilder.create().build()
  val httpResponse = httpClient.execute(new HttpGet(url))
  val entity = httpResponse.getEntity
    val str = EntityUtils.toString(entity, "UTF-8")
    val content = Json.parse(str)
  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[Nothing, (String,JsValue)](props)
  val topic = "quick-start"
  try {
      val record = new ProducerRecord(topic, content.as[JsObject].fields(1))
      producer.send(record)
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}