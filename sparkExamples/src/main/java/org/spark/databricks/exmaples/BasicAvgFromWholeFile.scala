package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

//local
//testdata\databricks\basicAvgFromWholeFile.txt
//testdata\databricks\basicAvgFromWholeOutputFile.txt

object BasicAvgFromWholeFile {
  
  def main(args : Array[String]) :Unit = {
    
    if(args.length < 2) {
      println("Usage : [sparkMaster] [inputDIrectory] [outputDirectory]")
      System.exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "BasicAvgFromWholeFile", System.getenv("SPARK_HOME"))
    
    val inputRdd = sc.wholeTextFiles(inputFile)
    inputRdd.foreach(println)
    val resultRDD = inputRdd.mapValues(y => {
      val nums = y.split(" ").map(_.toDouble)
      nums.sum /nums.size.toDouble
    } )
    
//    println("result is "+resultRDD)
    resultRDD.foreach(println)
//    val sum = resultRDD.sum /resultRDD.size.toDouble
    
    resultRDD.saveAsTextFile(outputFile)
    
  }
  
  
}