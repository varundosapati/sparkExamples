package org.udemy.spark.core

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/*
 * Usage 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/WordCount 
 */

object WordCount {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

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
    
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster(master)
    
    //Create spark Context using single core of machine with application name RatingCounter
    val sc = new SparkContext(sparkConf)
    
    val data = sc.textFile(inputLoc+"/book.txt")

    
    val flatData = data.flatMap(x => x.split(" "))
    
    flatData.map(x => (x, 1)).reduceByKey((x, y) => x+y).foreach(println)
    
    
  }
  
}