package pack

  import org.apache.spark.SparkContext  // rdd
  import org.apache.spark.sql.SparkSession  // dataframe
  import org.apache.spark.SparkConf
  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

object obj {
  
  def main(args:Array[String]):Unit={
  
  			val conf = new SparkConf().setMaster("local[*]").setAppName("first")
  
  					val sc = new SparkContext(conf)
  					sc.setLogLevel("ERROR")
  
  					val spark = SparkSession.builder.config(conf).getOrCreate()
  					import spark.implicits._
  
  					val schema = StructType(Array(
  							StructField("Order_ID",StringType,true),
  							StructField("User_Name",StringType,true),
  							StructField("Order_Time",StringType,true),
  							StructField("Order_Type", StringType, true),
  							StructField("Quantity", StringType, true),
  							StructField("Price", StringType, true)
  							))
  							
  					val buydf = spark
  					.read
  					.format("csv")
  					.schema(schema)
  					.load("file:///C:/coding_excercise/exampleOrders.csv")
  					
  					buydf.show()
  										
  					println("======BUY Data=========")
  					
  				  val dfbuy=buydf.where(buydf("Order_Type")==="BUY")
  				  .withColumnRenamed("Order_ID", "Buy_Order_ID")
  				  .withColumnRenamed("User_Name", "Buy_User_Name")
  				  .withColumnRenamed("Order_Time","Buy_Order_Time")
  				  .withColumnRenamed("Order_Type","Buy_Order_Type")
  				  .withColumnRenamed("Quantity","Buy_Quantity")
  				  .withColumnRenamed("Price", "Buy_Price")
  				  				  
  				 	dfbuy.show()
  					
  					println("======SELL Data=========")
  					
  					val dfsell=buydf.where(buydf("Order_Type")==="SELL")
  				  .withColumnRenamed("Order_ID", "Sell_Order_ID")
  				  .withColumnRenamed("User_Name", "Sell_User_Name")
  				  .withColumnRenamed("Order_Time","Sell_Order_Time")
  				  .withColumnRenamed("Order_Type","Sell_Order_Type")
  				  .withColumnRenamed("Quantity","Sell_Quantity")
  				  .withColumnRenamed("Price", "Sell_Price")
  				     
  					dfsell.show()
  					
  					println("======Matched Order Data=========")
  					
  					val dforder = dfbuy.join(dfsell,dfbuy("Buy_Quantity") === dfsell("Sell_Quantity"),"inner")
  					
  					dforder.show()
  					
  					println("======writing matched Order Data=========")
  					
  					dforder.write.format("csv").mode("overwrite").
  					save("file:///C:/coding_excercise/MatchedOrders")					
  																			                   
  					val dforderunmatched1 = dfbuy.join(dfsell,dfbuy("Buy_Quantity") === dfsell("Sell_Quantity"),"left")
  					.select("Buy_Order_ID", "Buy_User_Name","Buy_Order_Time","Buy_Order_Type","Buy_Quantity","Buy_Price","Sell_Order_ID")
  																			                   
  					val buyorderum =dforderunmatched1.where(dforderunmatched1("Sell_Order_ID") isNull)
  							.withColumnRenamed("Buy_Order_ID","Order_ID") 
  					    .withColumnRenamed("Buy_User_Name", "User_Name")
  					    .withColumnRenamed("Buy_Order_Time", "Order_Time")
  					    .withColumnRenamed("Buy_Order_Type" , "Order_Type")
  					    .withColumnRenamed("Buy_Quantity", "Quantity")
  					    .withColumnRenamed("Buy_Price" , "Price")
  					    .drop("Sell_Order_ID")
  					    										
  					buyorderum.show()
  					
  					val dforderunmatched2 = dfbuy.join(dfsell,dfbuy("Buy_Quantity") === dfsell("Sell_Quantity"),"right")
  					.select("Sell_Order_ID", "Sell_User_Name","Sell_Order_Time","Sell_Order_Type","Sell_Quantity","Sell_Price","Buy_Order_ID")
  																			                   
  					val buyorderum1 =dforderunmatched2.where(dforderunmatched1("Buy_Order_ID") isNull)
  					.withColumnRenamed("Sell_Order_ID","Order_ID") 
  					    .withColumnRenamed("Sell_User_Name", "User_Name")
  					    .withColumnRenamed("Sell_Order_Time", "Order_Time")
  					    .withColumnRenamed("Sell_Order_Type" , "Order_Type")
  					    .withColumnRenamed("Sell_Quantity", "Quantity")
  					    .withColumnRenamed("Sell_Price" , "Price")
  					    .drop("Buy_Order_ID")
  					    
  					 buyorderum1.show()
  					
  					val finalumorders = buyorderum1.union(buyorderum)
  					
  					println("======Un-Matched Order Data=========")
  					
  					finalumorders.show()
  					
  					println("======writing un-matched Order Data=========")
  					
  				finalumorders.write.format("csv").mode("overwrite").
  				save("file:///C:/coding_excercise/Un-MatchedOrders")
  	}
}