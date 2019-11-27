package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import au.com.bytecode.opencsv.CSVReader
import play.api.libs.json._
import java.io.StringReader

/*
 * Explanation - BasicParseCsv uses opencsv jar to convers csv file to DarkPoolData
 * 
 * DarkPoolData(ticket:String, industry: String, sector: String, volume:String) {RDD[DarkPoolData]:darkPoolDataRdd}
 * 
 * And then converting {RDD[darkPoolDataRdd]:darkPoolDataRdd} to {List[DarkPoolDataRdd]:darkPoolFilteredDataList}
 * 
 * 
 * For Converting T to a JSON we need to create implicit object , So created dataPoolDataWrites, dataPoolDataWritesList
 * 
 * And then we can call Json.toJson using play framework
 * 
 */

object BasicParseCsv {
  
  def main(args: Array[String]) : Unit = {
    
    val master = args.length match {
      case x :Int if x > 0 => args(0)
      case _ => "local[1]"
    }

    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "BasicParseCsv", System.getenv("SPARK_HOME"))
    
    val input = sc.textFile(inputFile)
    
    val result = input.map{x => {
          val reader = new CSVReader(new StringReader(x))
          reader.readNext()
      }
    }
  
//    result.foreach(println)
    val darkPoolDataRdd = result.map(x => {
//      if( x != null ) {
        DarkPoolData(x(2), x(9), x(10), x(5))
//      }
      
    } )
    
//    darkPoolDataRdd.foreach(println)

    
    val darkPoolFilteredDataList:List[DarkPoolData] = darkPoolDataRdd.collect().toList
    
    val darkPoolDataList : DarkPoolDataList = DarkPoolDataList(darkPoolFilteredDataList)
    
    implicit val dataPoolDataWrites = new Writes[DarkPoolData] {
      def writes(darkPoolData:DarkPoolData) = Json.obj(
        "tick" -> darkPoolData.ticket,
        "industry" -> darkPoolData.industry,
        "sector" -> darkPoolData.sector,
        "vol" -> darkPoolData.volume
      )
    }

    
    implicit val dataPoolDataListWrites = new Writes[DarkPoolDataList] {
      def writes( darkPoolList : DarkPoolDataList )= Json.obj( 
        "darkPoolList" -> darkPoolList.list  
      )
      
    }
    
//    val darkPoolDataFilteredData  =  darkPoolDataRdd.map( x => {
//      val splitData = x.volume.split("%")
//      DarkPoolData(x.ticket, x.industry, x.sector, splitData(0))
//    })
    
    val jsonStr = Json.toJson(darkPoolDataList).toString()
    
    
    println("JsonString is "+jsonStr)
  }

   case class DarkPoolData(ticket:String, industry: String, sector: String, volume:String)
  
  case class DarkPoolDataList(list: List[DarkPoolData])

}