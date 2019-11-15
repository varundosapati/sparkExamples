

package org.conseus_2010.colorado.sparkExamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.commons.math3.genetics.Population

object RankCountiesBySexUsingSQL {
  
   case class Geo(logrecno:String, name: String, sumlev :String)
  
  case class Population(logrecno:String, male: Int, female:Int  )
  
  case class CountyPopulation(county:String, male:Int, female:Int)
  
  def main(args : Array[String]) : Unit = {
    
     val spark = SparkSession.builder().master("local[*]").appName("RankCountiesBySexUsingSQL").getOrCreate();
    
    import spark.implicits._
    
     var geo: Dataset[Geo] = spark.read.text("testData/usconsesus/2010/colorado/cogeo2010.sf1")
                              .map(row => Geo(row.getString(0).substring(18, 25) , 
                                              row.getString(0).substring(226, 316).trim(), 
                                              row.getString(0).substring(8, 11))).alias("geo")
                               .filter(geo => geo.sumlev == "050")
    
                               
                               
    val pop:Dataset[Population] = spark.read.text("testData/usconsesus/2010/colorado/co000182010.sf1")
                                  .map(row  => row.getString(0).split(","))
                                  .map(csv => Population(csv(4) ,csv(6).toInt, csv(30).toInt ))
                                  .alias("pop")
           
    geo.registerTempTable("geo")
    pop.registerTempTable("pop")    
    
    
    
    spark.sql(
      "SELECT geo.name, pop.male, pop.female, pop.male/pop.female as m2f " +
      "FROM geo JOIN pop ON geo.logrecno = pop.logrecno " +
      "WHERE geo.sumlev = '050' " +
      "ORDER BY m2f LIMIT 10"
    ).collect().foreach(println)
                                  
  }
  
  
}