package pro.boto.recommender.ingestion

import java.util.UUID

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import pro.boto.recommender.configuration.{KafkaConfig, RecommenderConfig}

object KafkaFactory {

  import scala.collection.JavaConversions._

  private val logger = Logger.getLogger(getClass)

  private val config:KafkaConfig = RecommenderConfig.obtainKafkaConfig()

  private val producer: KafkaProducer[String,String] = {

    val params = Map[String, Object](
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> config.host()
    )

    new KafkaProducer[String, String](params)

  }

  def getOrCreateProducer(): KafkaProducer[String,String] = {
    return producer
  }

  def getOrCreateConsumer(streaming: StreamingContext): DStream[(String)] = {
    val topics = Array(config.topic())

    val kafkaParams = Map[String, Object](
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "bootstrap.servers" -> config.host(),
      "group.id" -> UUID.randomUUID().toString//config.groupId()
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streaming,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    return kafkaStream.map(record => record.value)
  }
}
