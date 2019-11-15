package org.conseus_2010.colorado.sparkExamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object RankCountiesBySexRDD {

    case class Geo(logrecno: String , name:String, sumlev:String)

case class Population(logrecno:String, male: Int, female: Int)

case class CountyPopulation(county:String, male:Int, female:Int)

  
  
  def main(args: Array[String]) {
    
      val sparkSession = SparkSession.builder().master("local[*]").appName("SparkExample").getOrCreate();
  
 
      
//      var rowRDD = sparkSession.sparkContext.textFile("testData/usconsesus/2010/colorado/cogeo2010.sf1").map(row => row.substring(10, 20))
//      
//      rowRDD.foreach(println)
      
      
     import sparkSession.implicits._
  
      
  var geoRDD : RDD[Geo] = sparkSession.sparkContext.textFile("testData/usconsesus/2010/colorado/cogeo2010.sf1")
  .map(row => Geo(row.substring(18, 25), row.substring(226, 316).trim, row.substring(8, 11) ))
  .filter(geo => geo.sumlev  == "050")
  
  geoRDD.foreach(println)

  val populationRDD : RDD[Population]  = sparkSession.sparkContext.textFile("testData/usconsesus/2010/colorado/co000182010.sf1")
                      .map(row => row.split(","))
                      .map(csv => Population(csv(4) , csv(6).toInt, csv(30).toInt))
                      
  populationRDD.foreach(println)
  
  //create pair RDDs
  
  val geoPair : RDD[(String, Geo)] = geoRDD.map(geo => (geo.logrecno, geo))
  val poppair : RDD[(String, Population)] = populationRDD.map(pop => (pop.logrecno, pop))
  
  //join 
  
  val join: RDD[(String, (Geo, Population))] = geoPair.join(poppair)
  
  //flatten 
  
  val flatternRDD :RDD[CountyPopulation] = join.map( (tuple : (String, (Geo, Population))) => CountyPopulation(tuple._2._1.name, tuple._2._2.male, tuple._2._2.female) )
  
 //show top N 
  
  flatternRDD.sortBy((p: CountyPopulation) => p.male*1.0f/p.female, ascending = true).zipWithIndex().filter( (t : (CountyPopulation, Long)) => t._2 < 10).foreach(println)
  
  
  
  }

  

}  


