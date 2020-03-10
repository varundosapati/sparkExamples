package org.udemy.spark.core

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/MostPopularSuperHero 
 */

object MostPopularSuperHero {
  
  //Function to extract the hero ID and number of occurances from each line 
  
  def countNoOccurences(line :String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length-1 )
  }
  
  //Function to extract heroId -> hero name tuples (or None in case of failure)
  def parseNames(line:String) : Option[(Int, String)] ={
    
    var fields = line.split('\"') // Using " quote to separate the data
    if(fields.length > 1){
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      None //flatMap will discard the None results and extract data from Some results
    }
  }
  
  def main(args : Array[String]) :Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    if(args.length < 2) {
      println("USAGE MASTER INPUTLOC")
      System.exit(0)
    }
    
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local[0]"
    }
    
    val inputLoc = args.length match {
      case y if y > 1 => args(1) 
      case _ => "testdata/udemy/spark/core/input/MostPopularSuperHero"
    }
  
    val sparkConf = new SparkConf().setMaster(master).setAppName("MostPopularSuperHero")
    
    val sparkContext = new SparkContext(sparkConf)
    
    //build hero name rdd
    val nameData = sparkContext.textFile(inputLoc+"/marvel-names.txt")
    val namesRDD = nameData.flatMap(parseNames)
    
    
    //Load up the superHero coapperance data
    val heroApperanceData = sparkContext.textFile(inputLoc+"/marvel-graph.txt")
    val heroApperanceRdd = heroApperanceData.map(countNoOccurences)
  
    val totalApperanceByCharacter = heroApperanceRdd.reduceByKey((x, y) => (x+y))
    
    val flipped = totalApperanceByCharacter.map(x => (x._2, x._1))
    
    
    

    val max = flipped.max()
    
     val mostPopularName = namesRDD.lookup(max._2)(0)
     val noApperance = max._1
     println(s"$mostPopularName is the most popular super her has $noApperance apperances")

     //For printing the top 10  super heros we are going to use the loop 
     
     val top10SuperHero = flipped.take(10)
     
     for(superHeroRecord <- top10SuperHero) {
        var heroName = namesRDD.lookup(superHeroRecord._2)(0)
        var noApperance = superHeroRecord._1
       println(s"$heroName is the most popular super her has $noApperance apperances")
     }
     
//     for(superHeroRecord <- flipped) {
//        var heroName = namesRDD.lookup(superHeroRecord._2)(0)
//       println(s"$heroName is the most popular super her has $superHeroRecord._1 apperances")
//     }
     
  }
  
}