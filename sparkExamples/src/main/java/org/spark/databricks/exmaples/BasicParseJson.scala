package org.spark.databricks.exmaples

import play.api.libs.json.Json
import play.api.libs.json.Format
import org.apache.spark.SparkContext
import play.api.libs.json.JsValue
import org.apache.spark.rdd.RDD

/*
 * 
 * TODO : Need to look in more about this 
 * Explaination : 
 */

object BasicParseJson {
 
  case class DarkPoolData(tick:String, industry: String, sector: String, vol:String)
  
  case class DarkPoolDataList(darkPoolList: List[DarkPoolData])

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
  
      
  implicit val darkPoolDataReads = Json.format[DarkPoolData]
  
  implicit val darkPoolDataListReads = Json.format[DarkPoolDataList]
    
    //We use asOpt combined with flatMap so that if it fails to parse we 
    //get back a None and the flatMap essentially skips the results 
    
    val result: RDD[DarkPoolData] = input.flatMap(record => darkPoolDataReads.reads(Json.parse(record)).asOpt)
    
    val listRdd = result.filter(x => x.tick=="APPL").map(Json.toJson(_)).saveAsTextFile(outputFile)
    
//   val eachRecordResult =  result.flatMap(record => darkPoolDataReads.reads(Json.parse(record)).asOpt)
//    result.
  }
  
}