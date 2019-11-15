package org.spark.examples.summit

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object WikipediaReadFIleWIthDataSet {
  
  case class wikiRecord (project:String, page:String, numRequests:Int )
  
  
  def main(obj : Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("wikipediaReadFileWithDataSet").getOrCreate()
    
    import spark.implicits._
    
    val rdd  = spark.read.textFile("testData/wikipediapageCount.gs")
    
//    val parsedRdd : RDD[wikiRecord] = rdd.as(RDD[wikiRecord]) 
    
    
  }
  
  
}