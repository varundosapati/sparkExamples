package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


/*
 * Usage 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/MaxTemparatures 
 */
 
object MaxTemparatures {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  
  def parsedData(line:String) ={
    
    val fields = line.split(",")
    val stationId = fields(0)
    val stattype = fields(2)
    val temp = fields(3).toFloat * 0.1f *(9.0f / 5.0f) + 32.0f
    (stationId, stattype, temp)
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
    
    val sparkConf = new SparkConf().setAppName("MaxTemparatures").setMaster(master)
    
    //Create spark Context using single core of machine with application name RatingCounter
    val sc = new SparkContext(sparkConf)
    
    val data = sc.textFile(inputLoc+"/1800.csv")
   
    val parsed = data.map(parsedData)

    
    val maxTempData = parsed.filter(x => x._2 == "TMAX")
    
    val stationIdMaxTemp = maxTempData.map(x => (x._1, x._3))
    
    val maxTempForStation = stationIdMaxTemp.reduceByKey((x, y) => Math.max(x, y))
    
    for(result <- maxTempForStation.sortBy(_._1)) {
      val stationId = result._1
      val temp = result._2
      val formatedTemp = f"$temp%.2f F"
      println(s"$stationId station max temp $formatedTemp")
      
    }
    
    
  } 
}