package pro.boto.recommender.ingestion.events

import java.sql.Timestamp

import org.apache.kudu.spark.kudu.{KuduContext, KuduDataFrameReader}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import pro.boto.recommender.configuration.{KuduConfig, RecommenderConfig}
import pro.boto.recommender.data.tables.event.{EventColumn, EventRow}
import pro.boto.recommender.ingestion.SparkFactory


object EventTables {
  private val kuduConfig:KuduConfig = RecommenderConfig.obtainKuduConfig

  private val kuduContext = new KuduContext(kuduConfig.master)

  val tableName: String = "userevents"

  def save(events: DStream[EventRow]): Unit = {
    if(!kuduContext.tableExists(tableName)) {
      println("table not exists")
      return
    }
    events.foreachRDD(event => {
      val spark = SparkFactory.getOrCreateSession()
      import spark.implicits._
      kuduContext.upsertRows(event.toDF(), tableName)
    })
  }


  def read(): DataFrame = {
    val sparkSession = SparkFactory.getOrCreateSession()
    import sparkSession.implicits._

    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> tableName,
      "kudu.master" -> kuduConfig.master)

    return sparkSession.read.options(kuduOptions).kudu
                       .orderBy(col(EventColumn.eventTime).desc)
                       .map(t => mapFrom(t)).toDF()
  }

  def mapFrom(t:Row): EventRow = {
    val sparkSession = SparkFactory.getOrCreateSession()
    val time =  t.getAs[Timestamp](EventColumn.eventTime)
    return new EventRow(t.getAs(EventColumn.sessionId),
                        time.getTime,
                        t.getAs(EventColumn.userId),
                        t.getAs(EventColumn.productId),
                        t.getAs(EventColumn.action))
  }


}
