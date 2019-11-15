package org.conseus_2010.colorado.sparkExamples

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object RankCountiesBySexUsingDataset2 {
  
  case class Geo(logrecno:String, name: String, sumlev :String)
  
  case class Population(logrecno:String, male: Int, female:Int  )
  
  case class CountyPopulation(county:String, male:Int, female:Int)
  
  def main(obj :Array[String]) :Unit = {
    
    //Spark 1.0
    //Set up the spark configuration and create contexts
//    val sparkConf = new SparkConf().setAppName("RankCountiesBySexUsingDataSet1.0").setMaster("local")
    //your handle to SparkContext to access other context like SQLContext
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    
    
    //Spark 2.0
    val spark = SparkSession.builder().master("local[*]").appName("RankCountiesBySexUsingDataSet").getOrCreate();
    
    import spark.implicits._
    
//    var foo: Dataset[String] = spark.createDataset(List("one", "two", "three"))
    
//    val dataset: Dataset[Geo] = spark.read.option("header", true).csv("testData/usconsesus/2010/colorado/cogeo2010.sf1").as[Geo]
    
//    dataset.show()
    
    
    var geo: Dataset[Geo] = spark.read.text("testData/usconsesus/2010/colorado/cogeo2010.sf1")
                              .map(row => Geo(row.getString(0).substring(18, 25) , 
                                              row.getString(0).substring(226, 316).trim(), 
                                              row.getString(0).substring(8, 11))).alias("geo")
                               .filter(geo => geo.sumlev == "050")
    
                               
                               
    val pop:Dataset[Population] = spark.read.text("testData/usconsesus/2010/colorado/co000182010.sf1")
                                  .map(row  => row.getString(0).split(","))
                                  .map(csv => Population(csv(4) ,csv(6).toInt, csv(30).toInt ))
                                  .alias("pop")
                                  
                                  
    val join : Dataset[(Geo, Population)] = geo.joinWith(pop, expr("geo.logrecno = pop.logrecno")).alias("join")                             
    
    
    join.printSchema()
    
    join.collect().foreach(println)
    
    
    val modifiedJoin : Dataset[(String, (Geo, Population))] = join.map(data => (data._1.logrecno,  (data)))

    modifiedJoin.collect().foreach(println)
    
    
    val flatten : Dataset[CountyPopulation] = join.map( data => CountyPopulation(data._1.name,  data._2.male, data._2.female) )
    
    val modifiedFlatten : Dataset[ (CountyPopulation, Float) ] = flatten.map(x => (x,  (x.male*1.0f/ x.female) ) ).filter(t => t._2 < 10.0)
    
    modifiedFlatten.collect().foreach(println) 
    
    
    
    //example of map with compile-time type saftey
    
//   val popContent =  join.map((tuple : (Geo, Population)) => tuple._2.male > 10000 )
//
//   popContent.printSchema()
//    
//   popContent.collect().foreach(println)
    
  }
  
}