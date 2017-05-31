package pro.boto.recommender.ingestion.actions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, max, sum}
import pro.boto.recommender.data.tables.action.{ActionColumn, ActionRow}
import pro.boto.recommender.data.tables.event.{EventColumn, EventRow}
import pro.boto.recommender.ingestion.SparkFactory
import pro.boto.recommender.ingestion.events.EventTables


object Actions {
    def main(args: Array[String]): Unit = {

      val sparkSession = SparkFactory.getOrCreateSession()
      import sparkSession.implicits._

      val actions = EventTables.read()
        .where(col(EventColumn.userId).isNotNull && col(EventColumn.productId).isNotNull)
        //.map(row => EventTables.mapFrom(row))
        .map(row => calculateAction(row))
        .groupBy(col(ActionColumn.userId), col(ActionColumn.productId))
        .agg(
          max(ActionColumn.lastAction).as(ActionColumn.lastAction),
          max(ActionColumn.reaction).as(ActionColumn.reaction),
          sum(ActionColumn.views).as(ActionColumn.views),
          sum(ActionColumn.contacts).as(ActionColumn.contacts),
          sum(ActionColumn.shares).as(ActionColumn.shares))
        .map(p => new ActionRow(p.getAs[Long](ActionColumn.lastAction)*1000,
                                p.getAs(ActionColumn.userId),
                                p.getAs(ActionColumn.productId),
                                p.getAs(ActionColumn.reaction),
                                p.getAs[Long](ActionColumn.views).toInt,
                                p.getAs[Long](ActionColumn.contacts).toInt,
                                p.getAs[Long](ActionColumn.shares).toInt))

      ActionTables.save(actions.toDF())
    }

  private def calculateAction(event: Row): ActionRow =  {
    var eventTime:Long = event.getAs(EventColumn.eventTime)
    var userId:String = event.getAs(EventColumn.userId)
    var productId:java.lang.Long = event.getAs(EventColumn.productId)
    var action:String = event.getAs(EventColumn.action)

    var contacts:Int = 0
    var shared:Int = 0
    var views:Int = 0
    var reaction:String = null

    if ("PROPERTY_CONTACTED".equals(action)) contacts=1
    if ("PROPERTY_SHARED".equals(action)) shared=1
    if ("PROPERTY_VIEWED".equals(action)) views=1
    if ("PROPERTY_SAVED".equals(action)) reaction="POSITIVE"
    if ("PROPERTY_DISCARTED".equals(action)) reaction="NEGATIVE"
    if ("PROPERTY_ROLLBACK".equals(action)) reaction="NEUTRAL"

    return ActionRow(eventTime,userId,productId,reaction,views,contacts,shared)
  }
}
