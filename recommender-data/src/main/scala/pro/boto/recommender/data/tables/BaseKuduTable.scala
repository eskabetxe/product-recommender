package pro.boto.recommender.data.tables

import pro.boto.flink.connector.kudu.schema.KuduConnector
import pro.boto.recommender.configuration.{KuduConfig, RecommenderConfig}


trait BaseKuduTable {

  protected val kuduConfig:KuduConfig = RecommenderConfig.obtainKuduConfig

  protected val tableMaster: String = kuduConfig.master
  protected val tableReplicas: Integer = kuduConfig.replicas

  val kuduConnector = new KuduConnector(tableMaster)

}
