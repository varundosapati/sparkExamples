package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

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
    
    val result = input.flatMap{ case(_, txt) => 
        val reader = new CSVReader(new StringReader(txt));
        val content = reader.readNext()
        println(content)
        content
    }
    
    
    println(result.collect().map(_.toList).mkString("|"))
    
   }
    
  
}