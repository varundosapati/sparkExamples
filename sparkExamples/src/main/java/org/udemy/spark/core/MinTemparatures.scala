package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * Usage 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/MinTemparatures 
 
 */
/** Find the minimum temperature by weather station */


object MinTemparatures {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def parseLine(line:String) = {
    
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temparature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
  
    (stationID, entryType, temparature)
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
    
    val sparkConf = new SparkConf().setAppName("MinTemparatures").setMaster(master)
    
    //Create spark Context using single core of machine with application name RatingCounter
    val sc = new SparkContext(sparkConf)
    
    val data = sc.textFile(inputLoc+"/1800.csv")
    
    
    //Getting stationid, entryType, temprature in f
    val parsedData = data.map(parseLine)
    
    //Getting only the MinT
    val minTempData = parsedData.filter(x => x._2=="TMIN")
    
    //Now as minTempData has all minTemp data remove the 2 field stationId, Temperature
    
    val stationtemps = minTempData.map(x => (x._1, x._3.toFloat))
    
    //Now get the minimum temparature for the station
    
    val minTempForStation = stationtemps.reduceByKey((x, y) => Math.min(x, y))
    
    
    for(result <- minTempForStation.sortBy(_._1)) {
      val station = result._1
      val temp = result._2
      val formatedTemp = f"$temp%.2f F"
      println(s"$station minimum temprature $formatedTemp")
    }
    
  }
  
  
}