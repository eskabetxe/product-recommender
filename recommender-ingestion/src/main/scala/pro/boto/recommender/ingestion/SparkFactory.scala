package pro.boto.recommender.ingestion

import org.apache.spark.sql.SparkSession

object SparkFactory {

  val sparkSession = SparkSession.builder()
    .appName("recommender-ingestion")
    .master("local[*]")
    .config("spark.local.dir", "/home/boto/tmp")
    .config("spark.executor.memory", "6g")
    .getOrCreate();

  def getOrCreateSession(): SparkSession = {
    return sparkSession;
  }

}
