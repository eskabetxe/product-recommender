package pro.boto.recommender.data.tables.product


case class ProductRow(productId:Long,
                      districto:String,
                      concelho:String,
                      freguesia:String,
                      latitude:Double,
                      longitude:Double,
                      typology:String,
                      operation:String,
                      condiction:String,
                      bedrooms: Int,
                      bathrooms:Int,
                      contructedArea:Int,
                      plotArea:Int,
                      elevator:Boolean,
                      parking:Boolean)
