package pro.boto.recommender.data.tables.action

import org.apache.kudu.Type
import pro.boto.flink.connector.kudu.KuduException
import pro.boto.flink.connector.kudu.schema.{KuduColumn, KuduTable}
import pro.boto.recommender.data.tables.BaseKuduTable


object ActionTable extends BaseKuduTable {

  val tableName: String = "useractions"

  @throws[KuduException]
  val kuduTable: KuduTable =
    KuduTable.Builder.create(tableMaster, tableName, true, tableReplicas)
      .column(KuduColumn.Builder.create(ActionColumn.sessionId, Type.STRING).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(ActionColumn.eventTime, Type.UNIXTIME_MICROS).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(ActionColumn.userId, Type.STRING).nullable(true).build)
      .column(KuduColumn.Builder.create(ActionColumn.productId, Type.INT64).nullable(true).build)
      .column(KuduColumn.Builder.create(ActionColumn.reaction, Type.STRING).nullable(true).build)
      .column(KuduColumn.Builder.create(ActionColumn.views, Type.INT32).build)
      .column(KuduColumn.Builder.create(ActionColumn.contacts, Type.INT32).build)
      .column(KuduColumn.Builder.create(ActionColumn.shares, Type.INT32).build).build

}
