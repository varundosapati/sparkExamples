package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


/*
 * Usage 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/CustomerAmountSpent 
 */


object CustomerAmountSpent {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

    def parsedCustomerData(line:String) ={
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val orderNo = fields(1)
    val price = fields(2).toFloat
    (customerId, orderNo, price)
  }
  
  
    def main(args : Array[String]) :Unit = {
    
    if(args.length < 2){
       println("USAGE local[1] inputLoc")
      System.exit(0)
    }
    
    /*
     *  Means where we are running local[1] which says we are running locally using single thread, We can also specify * which means to use all the threads in the machine  
     */
    
    val master = args(0) 
    val inputLoc = args(1)
    
    val sparkConf = new SparkConf().setAppName("CustomerAmountSpent").setMaster(master)
    
    //Create spark Context using single core of machine with application name RatingCounter
    val sc = new SparkContext(sparkConf)
    
    val data = sc.textFile(inputLoc+"/customer-orders.csv")
    
    val parsedData = data.map(parsedCustomerData).map(x => (x._1, x._3)).reduceByKey((x, y) => (x+y).toFloat)
    
    for(result <- parsedData.sortBy(_._2)) {
      val customerId = result._1
      val totalSpend = result._2.toFloat
      val formatedSpend = f"$totalSpend%.2f F"
      println(s"$customerId customer data $formatedSpend")
      
    }
    
    parsedData.collect().foreach(println)
    
    
    
    
  }
    
}