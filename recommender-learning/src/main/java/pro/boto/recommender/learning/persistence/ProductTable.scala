package pro.boto.recommender.learning.persistence

import org.apache.kudu.spark.kudu.{KuduContext, KuduDataFrameReader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.boto.recommender.configuration.RecommenderConfig
import pro.boto.recommender.data.tables.product.{ProductColumn, ProductRow}


object ProductTable {

  private val kuduConfig = RecommenderConfig.obtainKuduConfig

  private val kuduContext = new KuduContext(kuduConfig.master)

  private val kuduOptions: Map[String, String] = Map(
                                      "kudu.table"  -> "products",
                                      "kudu.master" -> kuduConfig.master)

  def read(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    return sparkSession.read.options(kuduOptions).kudu
      .map(t => new ProductRow(t.getAs(ProductColumn.productId),
                                t.getAs(ProductColumn.districto),
                                t.getAs(ProductColumn.concelho),
                                t.getAs(ProductColumn.freguesia),
                                t.getAs(ProductColumn.latitude),
                                t.getAs(ProductColumn.longitude),
                                t.getAs(ProductColumn.typology),
                                t.getAs(ProductColumn.operation),
                                t.getAs(ProductColumn.condiction),
                                t.getAs(ProductColumn.bedrooms),
                                t.getAs(ProductColumn.bathrooms),
                                t.getAs(ProductColumn.contructedArea),
                                t.getAs(ProductColumn.plotArea),
                                t.getAs(ProductColumn.elevator),
                                t.getAs(ProductColumn.parking)))
      .toDF()
      .cache()
  }

}
