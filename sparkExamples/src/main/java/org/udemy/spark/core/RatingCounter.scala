package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * 
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/RatingCounter 
 */ 
object RatingCounter {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]) : Unit = {
   
    if(args.length < 2){
      println("USAGE local[1] inputLoc")
      System.exit(0)
    }
    
    /*
     *  Means where we are running local[1] which says we are running locally using single thread, We can also specify * which means to use all the threads in the machine  
     */
    
    val master = args(0) 
    val inputLoc = args(1)
    
    val sparkConf = new SparkConf().setAppName("RatingCounter").setMaster(master)
    
    //Create spark Context using single core of machine with application name RatingCounter
    val sc = new SparkContext(sparkConf)
   
    //Load up rating data into lines as RDD
     val lines = sc.textFile(inputLoc)

     //Convert each line into String and split based on tab and get the third value 
     //the file formt is userId, movieId, rating, timeStamp
     val ratings = lines.map(x => x.split("\t")(2))

     //Now count each rating has occured
     val result = ratings.countByValue()
     
     //Sort the resulting map of (ratings, count) tuples
     val sortResults = result.toSeq.sortBy(_._1)
     
     println("Printing resulted sorted ratings")
     sortResults.foreach(println)
     
  }
  
  
}