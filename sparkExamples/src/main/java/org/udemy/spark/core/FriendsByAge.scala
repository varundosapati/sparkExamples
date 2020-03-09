package org.udemy.spark.core

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf

/*
 * Usage Explainination 
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/FriendsByAge 
 */
object FriendsByAge {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def parseLine(line :String) = {
    
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }
  
  def parsedLineFirstName(line :String) = {
    
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt
    (name, numFriends)
  }
  
  def main(args : Array[String]) :Unit ={
    
    if(args.length < 2){
      println("USAGE MASTER INPUTLOC")
      System.exit(0)
    }
    
    val master = args(0)
    val inputLoc =args(1)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("FriendsByAge")
    
    val sc = new SparkContext(sparkConf)
    
    val inputText = sc.textFile(inputLoc+"/fakeFriends.csv")
    
    val parsedData = inputText.map(x => parseLine(x))
    
    //Get the total number of friends by age 
    val totalFriendsByAge = parsedData.reduceByKey((x, y) => (x+y) )
    
    //Now sort by age to see number of friends for each age 
    val sortByAge = totalFriendsByAge.sortBy(_._1)
    println(" Total friends for each age")
    sortByAge.foreach(println)
  
    //Getting avearage of friends by each age 
    
    /*
     * 20 100
     * 20 10
     * 
     * 
     */
    val totalByAge = parsedData.mapValues(x => (x, 1)).reduceByKey((x, y) => ((x._1+y._1) , (x._2+ y._2)))
    /*
     * (20, (110, 2))
     */
     
    val avgOfFriendsByAGe = totalByAge.mapValues(x => x._1/x._2)
    
    
    val sortTheAvgFriendsAge = avgOfFriendsByAGe.sortBy(_._1)
    println("Getting the avegarte of friends for each age ")
    sortTheAvgFriendsAge.foreach(println)
    
    
    
    
    
    //Example of using aggregate 
    
//    val initalvalue = 0;
//    val addToCounts = (x:Int, v:Int) => x+1
//    val sumOfAdditionCounts = (p1:Int, p2:Int) => p1+p2
//    
//  val countByval =    parsedData.aggregateByKey(initalvalue)(addToCounts, sumOfAdditionCounts)    
//  
//  println("Aggregate by value what is given by")
//  countByval.foreach(println)
    
    
    val parsedDataWithFirstName = inputText.map(x => parsedLineFirstName(x))
    
    val totalByName = parsedDataWithFirstName.mapValues(x => (x, 1)).reduceByKey((x, y) => ((x._1+y._1) , (x._2+ y._2)))
    /*
     * (20, (110, 2))
     */
     
    val avgOfFriendsByName = totalByName.mapValues(x => x._1/x._2)
    
    
    val sortTheAvgFriendsName = avgOfFriendsByName.sortBy(_._1)
    println("Getting the avegarte of friends for each name ")
    sortTheAvgFriendsName.foreach(println)
    
  }
  
  
}