package pro.boto.recommender.learning.domain

import org.apache.spark.sql.Row
import pro.boto.recommender.data.tables.action.ActionColumn

object Rater {

  val USER_COLUMN = "userId"
  val PRODUCT_COLUMN = "productId"
  val RATING_COLUMN = "rating"

  case class Rating(userId:Long, productId:Long, rating:Float)

  def obtainRating(row:Row): Rating = {
    var rating:Float = calculateRating(row)
    return new Rating(row.getAs[String](ActionColumn.userId).replace("U","").replace("C","").toLong,
                      row.getAs(ActionColumn.productId),
                      rating)
  }

  private def calculateRating(row:Row): Float = {
    var rating = 0L
    val reaction = row.getAs[String](ActionColumn.reaction)
    var contacts = row.getAs[Long](ActionColumn.contacts)
    var shares   = row.getAs[Long](ActionColumn.shares)
    var views    = row.getAs[Long](ActionColumn.views)

    if("POSITIVE".equalsIgnoreCase(reaction)){
      rating = 5
    }else if("NEGATIVE".equalsIgnoreCase(reaction)) {
      rating = 1
    }else if(contacts > 0){
      rating = 4
    }else if(shares > 0 ){
      rating = 3
    }else if(views > 0) {
      rating = 2
    }
    if(rating == 0 && "NEUTRAL".equalsIgnoreCase(reaction)){
      rating = 1
    }
    return rating
  }

  private def calculateRatingImplicity(row:Row): Float = {
    var rating = 0L
    val reaction = row.getAs[String](ActionColumn.reaction)
    var contacts = row.getAs[Long](ActionColumn.contacts)
    var shares   = row.getAs[Long](ActionColumn.shares)
    var views    = row.getAs[Long](ActionColumn.views)

    if("POSITIVE".equalsIgnoreCase(reaction)){
      rating = 100
    }else if("NEGATIVE".equalsIgnoreCase(reaction)) {
      rating = 0
    }else {
      rating = views + (shares*2) + (contacts*5)
    }
    if(rating == 0 && "NEUTRAL".equalsIgnoreCase(reaction)){
      rating = 1
    }

    return rating
  }
}
