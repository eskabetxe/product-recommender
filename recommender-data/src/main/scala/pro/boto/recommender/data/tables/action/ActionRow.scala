package pro.boto.recommender.data.tables.action

case class ActionRow(lastAction:Long, userId:String, productId:java.lang.Long,reaction:String, views:Int, contacts:Int, shares:Int)
