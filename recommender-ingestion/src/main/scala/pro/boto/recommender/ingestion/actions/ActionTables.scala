package pro.boto.recommender.ingestion.actions

import java.sql.Timestamp

import org.apache.flink.api.java.DataSet
import org.apache.kudu.spark.kudu.{KuduContext, KuduDataFrameReader}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import pro.boto.recommender.configuration.{KuduConfig, RecommenderConfig}
import pro.boto.recommender.data.tables.action.{ActionColumn, ActionRow}
import pro.boto.recommender.ingestion.SparkFactory


object ActionTables {
  private val kuduConfig:KuduConfig = RecommenderConfig.obtainKuduConfig

  private val kuduContext = new KuduContext(kuduConfig.master)


  def save(actions: DataFrame): Unit = {
    if(!kuduContext.tableExists(kuduConfig.actionsTable)) {
      println("table not exists")
      return
    }

      kuduContext.upsertRows(actions, kuduConfig.actionsTable)
  }


  def read(): DataFrame = {
    val sparkSession = SparkFactory.getOrCreateSession()
    import sparkSession.implicits._

    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduConfig.actionsTable,
      "kudu.master" -> kuduConfig.master)

    return sparkSession.read.options(kuduOptions).kudu
                       .orderBy(col(ActionColumn.lastAction).desc)
                       .map(t => mapFrom(t)).toDF()
  }

  def mapFrom(t:Row): ActionRow = {
    val sparkSession = SparkFactory.getOrCreateSession()
    return new ActionRow(t.getAs[Timestamp](ActionColumn.lastAction).getTime,
                        t.getAs(ActionColumn.userId),
                        t.getAs(ActionColumn.productId),
                        t.getAs(ActionColumn.reaction),
                        t.getAs(ActionColumn.views),
                        t.getAs(ActionColumn.contacts),
                        t.getAs(ActionColumn.shares))
  }


}
