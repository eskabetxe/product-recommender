package pro.boto.recommender.learning.utils

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import pro.boto.recommender.learning.domain.Rater


object UserRecommender {

  def calculateUserRecs(maxRecs: Int, model: ALSModel, users: Dataset[Long], products: Dataset[Long]): DataFrame = {

    import products.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val userProductsJoin = users.crossJoin(products)
    val userRating = userProductsJoin.map { row => new Rater.Rating(row.getAs[Long](0), row.getAs[Long](1), 0) }

    object RatingOrder extends Ordering[Row] {
      def compare(x: Row, y: Row) = y.getAs[Float](model.getPredictionCol) compare x.getAs[Float](model.getPredictionCol)
    }

    val recommendations = model.transform(userRating)
      .filter(col(model.getPredictionCol) > 0 && !col(model.getPredictionCol).isNaN )
      .map(p => new Rater.Rating(p.getAs[Long](model.getUserCol), p.getAs[Long](model.getItemCol), p.getAs[Float](model.getPredictionCol)))
      .orderBy(col(Rater.RATING_COLUMN).desc)

      .toDF()


    return recommendations
  }



}
