package pro.boto.recommender.ingestion.producer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import pro.boto.recommender.configuration.RecommenderConfig
import pro.boto.recommender.ingestion.{IngestConfig, KafkaFactory, SparkFactory}
import pro.boto.recommender.ingestion.IngestConfig.SotConfig

object Ingestion {

  def main(args: Array[String]): Unit = {

    val sot:SotConfig = IngestConfig.obtainSotConfig

    val sparkSession = SparkFactory.getOrCreateSession()

    val topic:String = RecommenderConfig.obtainKafkaConfig().topic()

    val lines = sparkSession.sparkContext
      .textFile(sot.basepath+"*"+ sot.filename)

    lines.foreach(line =>  {
        val producer = KafkaFactory.getOrCreateProducer()
        val message = new ProducerRecord[String,String](topic, line)
        producer.send(message)
    })

    println("total events added: "+lines.count())

  }


}
