package pro.boto.recommender.data.tables

import pro.boto.flink.connector.kudu.schema.KuduConnector
import pro.boto.recommender.data.ConfigData
import pro.boto.recommender.data.tables.rating.RatingTable.{kuduConfig, tableMaster}


trait BaseKuduTable {

  protected val kuduConfig: ConfigData.KuduConfig = ConfigData.obtainKuduConfig

  protected val tableMaster: String = kuduConfig.master
  protected val tableReplicas: Integer = kuduConfig.replicas

  val kuduConnector = new KuduConnector(tableMaster)

}
