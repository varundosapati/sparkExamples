package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

object BasicSaveSequenceFile {
   def main(args: Array[String]) {
     
     if(args.length < 2) {
       println("input format [local] [outputfilepath]")
       System.exit(1)
     }
     
      val master = args(0)
      val outputFile = args(1)
      val sc = new SparkContext(master, "BasicSaveSequenceFile", System.getenv("SPARK_HOME"))
      val data = sc.parallelize(List(("Holden", 3), ("Kay", 6), ("Snail", 2)))
      data.saveAsSequenceFile(outputFile)
    }
}