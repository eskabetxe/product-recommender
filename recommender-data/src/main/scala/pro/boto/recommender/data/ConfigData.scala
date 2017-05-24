package pro.boto.recommender.data

import pro.boto.recommender.configuration.Configurations

object ConfigData {
  private val configurations = Configurations.fromFiles("recommender-application.yaml")

  private val DEFAULT_KUDU = "default.kudu"

  trait KuduConfig {
    def master: String
    def replicas: Integer
  }

  def obtainKuduConfig: KuduConfig = configurations.bind(DEFAULT_KUDU, classOf[KuduConfig])


}

