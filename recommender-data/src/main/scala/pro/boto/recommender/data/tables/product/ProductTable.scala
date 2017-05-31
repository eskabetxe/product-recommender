package pro.boto.recommender.data.tables.product

import org.apache.kudu.Type
import pro.boto.flink.connector.kudu.KuduException
import pro.boto.flink.connector.kudu.schema.{KuduColumn, KuduTable}
import pro.boto.recommender.data.tables.BaseKuduTable


object ProductTable extends BaseKuduTable {

  val tableName: String = "products"

  @throws[KuduException]
  val kuduTable: KuduTable =
    KuduTable.Builder.create(tableMaster, tableName, true, tableReplicas)
      .column(KuduColumn.Builder.create(ProductColumn.productId, Type.INT64).key(true).rangeKey(true).build)
      .column(KuduColumn.Builder.create(ProductColumn.districto, Type.STRING).build)
      .column(KuduColumn.Builder.create(ProductColumn.concelho, Type.STRING).build)
      .column(KuduColumn.Builder.create(ProductColumn.freguesia, Type.STRING).build)
      .column(KuduColumn.Builder.create(ProductColumn.latitude, Type.DOUBLE).build)
      .column(KuduColumn.Builder.create(ProductColumn.longitude, Type.DOUBLE).build)
      .column(KuduColumn.Builder.create(ProductColumn.typology, Type.STRING).build)
      .column(KuduColumn.Builder.create(ProductColumn.operation, Type.STRING).build)
      .column(KuduColumn.Builder.create(ProductColumn.condiction, Type.STRING).nullable(true).build)
      .column(KuduColumn.Builder.create(ProductColumn.bedrooms, Type.INT32).build)
      .column(KuduColumn.Builder.create(ProductColumn.bathrooms, Type.INT32).build)
      .column(KuduColumn.Builder.create(ProductColumn.contructedArea, Type.INT32).build)
      .column(KuduColumn.Builder.create(ProductColumn.plotArea, Type.INT32).nullable(true).build)
      .column(KuduColumn.Builder.create(ProductColumn.elevator, Type.BOOL).nullable(true).build)
      .column(KuduColumn.Builder.create(ProductColumn.parking, Type.BOOL).nullable(true).build).build



/*
  def mapRow(t: Row): ProductRow = {
    return new ProductRow(t.getAs(productId),
                          t.getAs(districto),
                          t.getAs(concelho),
                          t.getAs(freguesia),
                          t.getAs(latitude),
                          t.getAs(longitude),
                          t.getAs(typology),
                          t.getAs(operation),
                          t.getAs(condiction),
                          t.getAs(bedrooms),
                          t.getAs(bathrooms),
                          t.getAs(contructedArea),
                          t.getAs(plotArea),
                          t.getAs(elevator),
                          t.getAs(parking))
  }

  def read(): Dataset[ProductRow] = {
    val sparkSession = SessionData.obtain()
    import sparkSession.implicits._

    return sparkSession.read.options(tableOptions).kudu
          .map(t => mapRow(t))
  }

  def distritos(): java.util.List[String] = {
    val sparkSession = SessionData.obtain()
    import sparkSession.implicits._

    return read()
      .orderBy(districto)
      .groupBy(districto)
      .count()
      .map(r => r.getAs[String](districto))
      .collectAsList()

  }
  */

}
