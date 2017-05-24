package pro.boto.recommender.data.tables.rating

import org.apache.kudu.Type
import pro.boto.flink.connector.kudu.KuduException
import pro.boto.flink.connector.kudu.schema.{KuduColumn, KuduConnector, KuduTable}
import pro.boto.recommender.data.ConfigData
import pro.boto.recommender.data.tables.BaseKuduTable

object RatingTable extends BaseKuduTable {

  val tableName: String = "userratings"

  @throws[KuduException]
  val kuduTable: KuduTable =
    KuduTable.Builder.create(tableMaster, tableName, true, tableReplicas)
      .column(KuduColumn.Builder.create(RatingColumn.userId, Type.STRING).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(RatingColumn.productId, Type.INT64).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(RatingColumn.rating, Type.FLOAT).build).build

}
