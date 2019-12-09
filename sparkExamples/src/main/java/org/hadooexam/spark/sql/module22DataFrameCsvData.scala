package org.hadooexam.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/*
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\input\module22DataFrameCsvData\data.csv
 * args(2) - testdata\hadoopexam\input\module22DataFrameCsvData\result
 * 
 * NOTE:
 * case class can only have 22 elements more that that scala cannot implement by default get/setter/toString/hashCode 
 * In dataframe float values cannot be nullable 
 * 
 */

object module22DataFrameCsvData {
  
  case class Data(acutionid : String , bid : Float, bidTime : Float, bidder: String, bidderRate : Integer, openBid : Float, price : Float, item : String, dayToLive : Integer)
  
  def main(args : Array[String]) : Unit = {
    

    if(args.length < 3) {
      println("USAGE local[1] inputFile outfile")
      System.exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "module22DataFramCsvData", System.getenv("SPARK_HOME"))
    
    val input = sc.textFile(inputFile)
    
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    import org.apache.spark.sql._
    
    val inputRdd  = input.map(_.split(",")).map(x => Data(x(0).toString, x(1).trim().toFloat, x(2).trim().toFloat, x(3).toString, x(4).trim().toInt, x(5).toFloat, x(6).trim().toFloat, x(7).toString, x(8).trim().toInt)) 
    
    
    val inputDf: DataFrame  = inputRdd.toDF()
    
    println("Records in dataFrame is ")
    inputDf.show()
    println("Printing first record in dataframe"+inputDf.first().mkString("|"))
    println("Printing total records in dataframe"+inputDf.count())
    
    
    println("print schema")
    inputDf.printSchema()
    
   println("Select all distinct auctionId count "+inputDf.select("acutionid").distinct().count())
    
   
   println("Select how many bids by item for each auctionId "+inputDf.groupBy("auctionid", "item").count())
   
   println("Show min count avg count and max count")
   
//   inputDf.groupBy("auctionid", "item").count().agg(min("count"), avg("count"), max("count"))
    
   val highPrice = inputDf.filter("price > 100")
   println("High price record in dataframe is ")
   highPrice.show()
   
   
   /*
    * Register dataframe as temporary table
    */
   
    inputDf.registerTempTable("datatable")
    
    //How many bids per auction 
    
    val results = sqlContext.sql("select auctionid, item, count(bid) from datatable GROUP BY auctionid, item")
    
    //display results

    results.show()
    
    //display max price for auctionid
    
   val maxresults = sqlContext.sql("select auctionid, MAX(price), FROM datatable GROUP BY item, auctionid")
    
    maxresults.show()
    
    val newResult = sqlContext.sql(" select auctionid, item, count(bid) from datatable GROUP BY auctionid, item").explain()
    
    
  }
  
  
}