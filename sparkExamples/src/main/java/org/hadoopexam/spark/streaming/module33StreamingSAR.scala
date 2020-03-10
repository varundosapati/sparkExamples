package org.hadoopexam.spark.streaming

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
/*
 * Before running this file make sure you run below command on linux machine 
 * 
 * sar -r 2 20 | nc -lk 9999
 * 
 * Usage - In this file we create an example to analyze memory activity in linux machine  
 * 
 * TODO: We need to find a way to do the same example on windows machine 
 */


object module33StreamingSAR {
  

  def main(args : Array[String]) : Unit = {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length < 1) {
      println("USAGE (local[1])")
      System.exit(1)
    }
    
    println("  Before running this file make sure you run below command on linux machine")
    println("sar -r 2 20 | nc -lk 9999")
    
    val master = args(0)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module33StreamingSAR").set("spark.files.overwrite", "true")

 val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import sparkSession.implicits._
    
    val myStream = sparkSession.read.format("socket").option("host", "localhost").option("port", 9999).load()
    
    
    /*
     * Filter out unwanted lines and then extract free memory pasrt as a float
     * Drop missing valies if any
     */
     
//     val sqlContext = new SQLContext(sparkSession.sparkContext)
//    import sqlContext.implicits._
   
    val myDF = myStream.filter($"value".contains("CST")).select(substring($"value", 15, 9).as("memFree")).na.drop().select($"memFree")
  
  //Define aggregate function
    
    val avgMemFree = myDF.select(avg("memeFree").cast("double"))
  
    val query = avgMemFree.writeStream.outputMode("complete").format("console").start()
    
    
  }
  
}