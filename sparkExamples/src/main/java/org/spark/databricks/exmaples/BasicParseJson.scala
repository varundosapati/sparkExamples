package org.spark.databricks.exmaples

import play.api.libs.json.Json
import play.api.libs.json.Format
import org.apache.spark.SparkContext
import play.api.libs.json.JsValue
import org.apache.spark.rdd.RDD

/*
 * Explaination : 
 */

object BasicParseJson {
 
  case class DarkPoolData(ticket:String, industry: String, sector: String, volume:String)
  
  case class DarkPoolDataList(list: List[DarkPoolData])
 
  implicit val darkPoolDataReads = Json.format[DarkPoolData]
  
  implicit val darkPoolDataListReads = Json.format[DarkPoolDataList]
  
  def main(args: Array[String]) :Unit = {
     
    if(args.length < 3) {
      println("USAGE [master] [inputFilePath] [outputFIlePath]")
      System.exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "BasicParseJson" , System.getenv("SPARK_HOME"))
    
    val input = sc.textFile(inputFile)
    
    
    //We use asOpt combined with flatMap so that if it fails to parse we 
    //get back a None and the flatMap essentially skips the results 
    
    val result: RDD[DarkPoolDataList] = input.flatMap(record => darkPoolDataListReads.reads(Json.parse(record)).asOpt)
    
//   val eachRecordResult =  result.flatMap(record => darkPoolDataReads.reads(Json.parse(record)).asOpt)
//    result.
  }
  
}