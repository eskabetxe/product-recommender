package pro.boto.recommender.learning.utils

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import org.jblas.DoubleMatrix
import pro.boto.recommender.learning.domain.Rater
import pro.boto.recommender.learning.persistence.SparkFactory


object ProductRecommender {

  def calculateProductRecs(maxRecs: Int, model: ALSModel) : DataFrame = {
    val sparkSession = SparkFactory.getOrCreateSession()
    import sparkSession.implicits._
   // import products.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    object RatingOrder extends Ordering[(Int, Int, Double)] {
      def compare(x: (Int, Int, Double), y: (Int, Int, Double)) = y._3 compare x._3
    }

    val recommendations = model.itemFactors.crossJoin(model.itemFactors)
      .filter(r => r.getAs[Int](0) != r.getAs[Int](2))
      .map { r =>
        val idA = r.getAs[Int](0)
        val idB = r.getAs[Int](2)
        val featuresA = r.getAs[mutable.WrappedArray[Float]](1).map(_.toDouble).toArray
        val featuresB = r.getAs[mutable.WrappedArray[Float]](3).map(_.toDouble).toArray

        (idA, idB, cosineSimilarity(new DoubleMatrix(featuresA), new DoubleMatrix(featuresB)))
      }
      .map(p => new Rater.Rating(p._1, p._2, p._3.toFloat))
      .filter(col(Rater.RATING_COLUMN) > 0 && !col(Rater.RATING_COLUMN).isNaN)
      .orderBy(col(Rater.USER_COLUMN),col(Rater.RATING_COLUMN).desc)
      .limit(3)

      /*
      .mapValues(cumsum).flatMapValues(lambda x:x)
      .map(p => new Rating(p._1, p._2, p._3.toFloat))
      .groupBy(col(RatingColumn.userId).desc)
      */



    return recommendations.toDF()
  }

  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }


}
