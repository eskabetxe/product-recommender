package pro.boto.recommender.data.tables.rating

case class RatingRow(userId: String, productId: Long, rating: Float)