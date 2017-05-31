package pro.boto.recommender.data.tables.event

import org.apache.kudu.Type
import pro.boto.flink.connector.kudu.KuduException
import pro.boto.flink.connector.kudu.schema.{KuduColumn, KuduTable}
import pro.boto.recommender.data.tables.BaseKuduTable


object EventTable extends BaseKuduTable {

  val tableName: String = "userevents"

  @throws[KuduException]
  val kuduTable: KuduTable =
    KuduTable.Builder.create(tableMaster, tableName, true, tableReplicas)
      .column(KuduColumn.Builder.create(EventColumn.sessionId, Type.STRING).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(EventColumn.eventTime, Type.UNIXTIME_MICROS).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(EventColumn.userId, Type.STRING).nullable(true).build)
      .column(KuduColumn.Builder.create(EventColumn.productId, Type.INT64).nullable(true).build)
      .column(KuduColumn.Builder.create(EventColumn.action, Type.STRING).nullable(true).build)
      .build

}
