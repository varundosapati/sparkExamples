package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

/*
 * Below are the arguments
 * 
 * args(0) - local
 * args(1) - testdata\databricks\basicAvgFromFile
 *  
 */

object BasicAvgFromFIle {
  
  def main(args: Array[String]) : Unit = {
   
    if(args.length < 2) {
      println("Usage: [sparkMaster] [inputFile]")
      System.exit(1)
    }
    
    val master = args(0)
    val inputfile = args(1)
    
    val sc = new SparkContext(master, "BasicAvgFromFile", System.getenv("SPARK_HOME"))
    
    val inputFileRdd = sc.textFile(inputfile)
    
//    val parsedRdd = inputfil
    
    inputFileRdd.foreach(println)
    
    inputFileRdd.map(_.toInt)foreach(println)
    
    val result = inputFileRdd.map(_.toInt).aggregate((0,0))((acc, value) => (acc._1+value, acc._2+1), (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
    
    
    printf("result content"+result)
    
    
  }
  
  
}