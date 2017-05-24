package pro.boto.recommender.data.tables.action

case class ActionRow(userId:String, productId:Long, reaction:String, views:Int, contacts:Int, shares:Int)
