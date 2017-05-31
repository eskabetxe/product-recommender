package pro.boto.recommender.learning.trainer

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.{col, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.boto.recommender.data.tables.action.ActionColumn
import pro.boto.recommender.learning.domain.Rater
import pro.boto.recommender.learning.persistence.{ActionTable, SparkFactory}
import pro.boto.recommender.learning.utils.ProductRecommender

object Trainer {

  case class ModelParam(iterations:Int, rank:Int, lambda:Double)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkFactory.getOrCreateSession()
    import sparkSession.implicits._

    println("calculating rating")
    val ratings = ActionTable.read()
      .where(col(ActionColumn.userId).isNotNull && col(ActionColumn.productId).isNotNull)
      .groupBy(col(ActionColumn.userId), col(ActionColumn.productId))
      .agg(max(ActionColumn.reaction).as(ActionColumn.reaction),
        sum(ActionColumn.views).as(ActionColumn.views),
        sum(ActionColumn.contacts).as(ActionColumn.contacts),
        sum(ActionColumn.shares).as(ActionColumn.shares))
      .map(p => Rater.obtainRating(p))
      .toDF()
      .cache()

    val pipeModel = Trainer.testing(ratings)

    //pipeModel.save("hdfs://172.25.0.2:9000/tmp")

    //Trainer.train(ratings, new ModelParam(20,50,0.1))

  }



  def testing(ratings: DataFrame): ModelParam = {

    println("calculating model")

    val splits = ratings.randomSplit(Array[Double](0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val ranks = List(50)
    val lambdas = List(0.1)
    val numIters = List(20)
    var bestParams: Option[ModelParam] = None
    var bestRmse = Double.MaxValue
    var bestTime: Long = 0
    var model:ALSModel = null
    for (rank <- ranks; numIter <- numIters; lambda <- lambdas) {

      val startTime = System.currentTimeMillis()
      val param = new ModelParam(numIter, rank, lambda)
      model =  modelTrain(ratings, param)

      println("testing model => rank = " + rank + " iter = " + numIter + " lambda = " + lambda )
      val MSE = computeRmse(model,test)

      val endTime = System.currentTimeMillis()
      if (MSE < bestRmse) {
        bestParams = Some(param)
        bestRmse = MSE
        bestTime = endTime - startTime
      }
    }
    println()
    println("<--- BEST RESULT --->");
    println("Params = " + bestParams.get.toString +" with RMSE " + bestRmse + " on " +bestTime/1000 +"s")

    val f =ProductRecommender.calculateProductRecs(3,model)
    f.take(10);

    return bestParams.get

  }


  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
  def computeRmse(model:ALSModel, data:DataFrame):Double = {
    """
    Compute RMSE (Root mean Squared Error).
    """
    val predictions = model
      .transform(data)
      .filter(row => !row.getAs[Float]("prediction").isNaN && !row.getAs[Float]("rating").isNaN)
    predictions.show(3)
    val rmse = evaluator.evaluate(predictions)
    println("Root-mean-square error = " + rmse)
    return rmse
  }


  def train(ratings: DataFrame, param: ModelParam): Unit = {

    val model = modelTrain(ratings, param)

    //model.save("/home/boto/tmp/model/save")
    //model.save("hdfs://172.25.0.2:9000/tmp")
   // model.write.overwrite().save()

  }

  def modelTrain(ratings: DataFrame, param: ModelParam): ALSModel = {
    val als = new ALS()
      .setMaxIter(param.iterations)
      .setRank(param.rank)
      .setRegParam(param.lambda)
      .setUserCol(Rater.USER_COLUMN)
      .setItemCol(Rater.PRODUCT_COLUMN)
      .setRatingCol(Rater.RATING_COLUMN)
  //  val stages = Array(als)
  //  val pipeline = new Pipeline().setStages(stages)

    return als.fit(ratings)
  }

}
