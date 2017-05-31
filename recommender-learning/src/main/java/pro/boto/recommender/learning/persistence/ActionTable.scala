package pro.boto.recommender.learning.persistence

import java.sql.Timestamp

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import pro.boto.recommender.configuration.{KuduConfig, RecommenderConfig}
import pro.boto.recommender.data.tables.action.{ActionColumn, ActionRow}
import org.apache.kudu.spark.kudu.{KuduContext, KuduDataFrameReader}


object ActionTable {
  private val kuduConfig:KuduConfig = RecommenderConfig.obtainKuduConfig

  private val kuduContext = new KuduContext(kuduConfig.master)



  def read(): DataFrame = {
    val sparkSession = SparkFactory.getOrCreateSession()
    import sparkSession.implicits._

    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduConfig.actionsTable,
      "kudu.master" -> kuduConfig.master)

    return sparkSession.read.options(kuduOptions).kudu
                       .orderBy(col(ActionColumn.lastAction).desc)
                       .map(t => mapFrom(t))
                       .toDF()
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
