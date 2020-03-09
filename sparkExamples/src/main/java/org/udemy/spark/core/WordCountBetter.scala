package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 * Usage 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/WordCount 
 */


object WordCountBetter {
  
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
    
    
    val text = data.flatMap(x => x.split("\\W+")).map(x => (x.toLowerCase(), 1)).reduceByKey((x, y) => (x+y)).sortBy(_._1) foreach(println)
   
    //Another way of using 
    
    val splittedData = data.flatMap(x => x.split("\\W+"))
    
   splittedData.map(x => (x.toLowerCase() , 1)).countByValue().foreach(println)
    
   val ignoreList = List("the", "is", "0", "000", "05")

   println("Filtered Data")
   val filteredData = splittedData.map(x => (x.toLowerCase(), 1)).filter(x => !ignoreList.contains(x._1)).reduceByKey((x, y) => (x+y)).foreach(println)
   
   } 
}