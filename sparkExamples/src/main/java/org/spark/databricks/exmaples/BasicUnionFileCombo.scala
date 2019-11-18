package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

object BasicUnionFileCombo {
 
  def main(args: Array[String]):Unit ={
    
      if(args.length < 2)  {
      println("Args [master] [inputFilename]")
      System.exit(1)
    }
      
      val master = args(0)
      val inputFIleName = args(1)
      
      val sc = new SparkContext(master, "BasicUnioinFIleCombo", System.getenv("SPARK_HOME"))
      
      
      val inputRdd = sc.textFile(inputFIleName)
      
      val errorRdd = inputRdd.filter(_.contains("error"))
      
      val warRdd = inputRdd.filter(_.contains("warn"))
      
      val badLineUnionRdd = errorRdd.union(warRdd)
      
      println(badLineUnionRdd.collect().mkString("\n"))
    
  }
  
  
}