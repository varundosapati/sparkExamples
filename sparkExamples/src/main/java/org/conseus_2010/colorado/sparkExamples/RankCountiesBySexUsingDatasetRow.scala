package org.conseus_2010.colorado.sparkExamples

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object RankCountiesBySexUsingDatasetRow {
//   case class Geo(logrecno:String, name: String, sumlev :String)
//  
//  case class Population(logrecno:String, male: Int, female:Int  )
//  
//  case class CountyPopulation(county:String, male:Int, female:Int)
  
  def main(args : Array[String]) : Unit = {
    
     val spark = SparkSession.builder().master("local[*]").appName("RankCountiesBySexUsingSQL").getOrCreate();
    
    import spark.implicits._
  
                                  
   }                               
}