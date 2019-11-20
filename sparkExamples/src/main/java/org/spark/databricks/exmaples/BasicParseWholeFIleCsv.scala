package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

object BasicParseWholeFIleCsv {
  
   def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "BasicParseWholeFileCsv", System.getenv("SPARK_HOME"))
    val input = sc.wholeTextFiles(inputFile)
    
    
    
   }
    
  
}