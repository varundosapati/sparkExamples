package org.spark.databricks.exmaples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/*
 * Explnation -  using streamingContext with socketTextStream to access port specified which is already running to get the logs
 * 
 */


object BasicStreamingExample {

  def main(args: Array[String]) : Unit ={
    if(args.length < 2) {
      System.err.println("Usage of BasicStramingExample <master> <outputFIle>")
      System.exit(1)
    }
    
    val Array(master, outputFile) = args.take(2)
    
    val conf = new SparkConf().setMaster(master).setAppName("BasicStreamingExample")
  
    val ssc = new StreamingContext(conf, Seconds(30))
    
    
    val lines = ssc.socketTextStream("127.0.0.1", 5354)
    
    val words = lines.flatMap(_.split(" "))
    
    val wc = words.map(x => (x, 1)).reduceByKey((x, y) => x+y)
    
    wc.saveAsTextFiles(outputFile)
    
    wc.print()
    
    println("pandas : sscstart")
    
    ssc.start()
    
    println("Pandas : Awaittermination")
    
    ssc.awaitTermination()
    
    println("pandas: done")
    
    
  }
  
  
}
