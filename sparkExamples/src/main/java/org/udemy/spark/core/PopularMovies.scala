package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


/*
 * 
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/RatingCounter 
 */ 

object PopularMovies {
  
  
  def main(args : Array[String]) :Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    if(args.length < 2) {
      println("USAGE MASTER INPUTLOC")
      System.exit(0)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("PopularMovies")
    
    val sc = new SparkContext(sparkConf)
    
    
    val inputData = sc.textFile(inputLoc+"/u.data")
    
    val movieIdMap = inputData.map(x =>( x.split("\t")(1).toInt , 1))
  
    val movieCount = movieIdMap.reduceByKey((x, y) => x+y)
    
    val filipped = movieCount.map(x => (x._2, x._1))

    val sortingData = filipped.sortByKey()
    
    println("Displaying how many times each movie is found in file")
    
    sortingData.collect().foreach(println)
  
  }
  
}