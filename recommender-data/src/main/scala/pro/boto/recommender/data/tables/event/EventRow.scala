package pro.boto.recommender.data.tables.event

case class EventRow(sessionId:String, eventTime:Long, userId:String, productId:java.lang.Long, action:String)
