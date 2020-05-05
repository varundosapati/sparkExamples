package org.hadoopexam.spark.core

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


/*
   * Usage Explanation
   * args(0) - local[1]
   * args(1) - testdata\hadoopexam\input\module19RemoveHeaderCsvAndMap\
   * args(2) - testdata\hadoopexam\output\module19RemoveHeaderCsvAndMap\
   * 
   */

object module19RemoveHeaderCsvAndMap {
  
  def main(args: Array[String]) : Unit = {
    
    if(args.length < 3) {
      println("USAGE local[1],  inputFIleLocation, outPutFileLocation")
//      System.exit(1)
    }
    
    val master = args.length match {
      case x:Int if x> 0 => args(0)
      case _ => "local[1]"
    } 
    val inputFile = args.length match {
      case x: Int if x > 1 => args(1)
      case _ => "testdata\\hadoopexam\\input\\module19RemoveHeaderCsvAndMap\\"
    }
    val outputFile = args.length match {
      case x : Int if x > 2 => args(2) 
      case _ => "testdata\\hadoopexam\\output\\module19RemoveHeaderCsvAndMap\\"
    } 
    
    val sc = new SparkContext(master, "module19RemoveHeaderCsvAndMap", System.getenv("SPARK_HOME"))
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val input = sc.textFile(inputFile)
    
    //Get each line to inputPairRdd separated by , and trim each tuple  
    val inputPairRdd = input.map(x => (x.split(",").map(_.trim)) )
    println("All records in input are")
    inputPairRdd.map(x => x).map(x => { 
      x.foreach(print)
      println("")  
    } )    
    //Get the first row
    val firstRdd = inputPairRdd.first
    
    println("First Row is")
    firstRdd.foreach(println)
    
    //Get the final data without first row
    val filteredRdd = inputPairRdd.filter(x => x(0) != firstRdd(0) )
    println("Filtered data with out first record")
    filteredRdd.map(x => x).foreach(x => println(x.mkString("|"))) 
    
    //Now zip the headerRecords with filteredRdd
    val map = filteredRdd.map(x => firstRdd.zip(x).toMap)
    println("Records after zipping with firstRdd is")
    
    map.collect().map(x => x).foreach(x => println(x.mkString("->")))
    
    //Now remove the "myself" of an id header 
    
    val finalResult = map.filter(map => map("id") != "myself")
    println("Result of records after removing of id with myself")
    finalResult.map(x => x).foreach(y => println(y.mkString("^")))
    
    finalResult.saveAsTextFile(outputFile)
  }
  
}