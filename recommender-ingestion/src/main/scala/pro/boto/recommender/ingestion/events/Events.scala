package pro.boto.recommender.ingestion.events

import org.apache.spark.streaming.{Seconds, StreamingContext}
import pro.boto.recommender.data.tables.event.EventRow
import pro.boto.recommender.ingestion.{KafkaFactory, SparkFactory}


object Events {
    def main(args: Array[String]): Unit = {

      val sparkSession = SparkFactory.getOrCreateSession()

      val streaming = new StreamingContext(sparkSession.sparkContext, Seconds(1))

      val eventStream = KafkaFactory.getOrCreateConsumer(streaming)

      val actionStream = eventStream
        .map(record =>json(record))

      EventTables.save(actionStream)

      streaming.start()
      streaming.awaitTermination()

    }


  case class EventSot(sessionId:String, eventTime:Long, userId:String, propertyId:java.lang.Long, eventAction:String)

  def json(value:String):EventRow = {
    import com.owlike.genson.defaultGenson._
    val sot = fromJson[EventSot](value)
    return new EventRow(sot.sessionId,sot.eventTime*1000,sot.userId,sot.propertyId,sot.eventAction)
  }
}
