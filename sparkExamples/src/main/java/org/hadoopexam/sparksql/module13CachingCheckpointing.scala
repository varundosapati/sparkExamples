package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/*
 * Usage - We are going indepth example of Dataset and DataFrame Caching and Checkpointing 
 * 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module13CachingCheckpointing\
 * args(2) - testdata\hadoopexam\sparkSql\output\module13CachingCheckpointing\
 *
 */

object module13CachingCheckpointing {
  
  case class Course(id:Int, name:String, fee:Int, venue:String, date: String, duration:Int)
  
  def main(args : Array[String]) : Unit = {
    if(args.length < 3) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
 
  val master = args(0)
  val inputLoc = args(1)
  val outputLoc = args(2)
  
  val sparkConf = new SparkConf().setMaster(master).setAppName("module13CachingCheckpointing")
  
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  import sparkSession.implicits._
  
  /*
   * 
   */
  
  val trainingDF = sparkSession.read.format("csv").option("header", true).option("inferschema", true).load(inputLoc+"Training.csv")

  
  val trainingDS = trainingDF.as[Course]

  trainingDS.cache()

  trainingDS.show()
  
  /*
   * After applying action there are two ways we can check if data is stored 
   * 
   * 1) Using UI (http://localmachineipaddress:sparkrunningportNumber/storage)
   * 2) Using command  queryExecution.withCachedData 
   */
  println("Cached data information "+trainingDS.queryExecution.withCachedData)
  
  trainingDS.unpersist()
  
  println("After unpersisting checking Cached data information "+trainingDS.queryExecution.withCachedData)
  
  /*
   * Example of checkpointing
   * NOTE: Checkpointing need to specify with output directory 
   *  
   */
  sparkSession.sparkContext.setCheckpointDir(outputLoc+"checkpointing")
  
  println("Getting extended explain plan "+trainingDS.select($"id", $"name").explain(true))
  
  trainingDS.checkpoint() // NOTE Checkpointing directory need to be created before we call this 
  
  println("Path of the checkpoint directory"+sparkSession.sparkContext.getCheckpointDir.get)
  
  
  //Debug to check 
  
  println("Debug checkpointing "+ trainingDS.queryExecution.toRdd.checkpoint())
  
  
  
  
  }
  
  
}