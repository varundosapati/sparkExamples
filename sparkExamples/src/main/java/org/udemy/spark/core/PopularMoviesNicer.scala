package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level


/*
 * 
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/RatingCounter 
 */


object PopularMoviesNicer {
 
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames1() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("testdata/udemy/spark/core/input/RatingCounter/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  def loadMovieNames() : Map[Int, String] = {
    
    //Handle Character encoding issues

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("testdata/udemy/spark/core/input/RatingCounter/u.item").getLines()
    
    for(line <- lines) {
      var fields = line.split('|') //having  "|" gave completely different result
      if(fields.length > 1) {
        var movieId = fields(0).toInt
        var movieName = fields(1)
        println(s"movie Id  $movieId and movie name $movieName")
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    println("MNovie name size is"+movieNames.size)
    
    return movieNames
  }
  
  
  def main(args : Array[String]) : Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    if(args.length < 2) {
      println("USAGE MASTER INPUTLOC")
      System.exit(0)
    }
    
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local[1]"
    }    
    
    val inputLoc = args.length match {
      case y if y > 1 => args(1)
      case _ => "testdata/udemy/spark/core/input/RatingCounter"
    }
  
  val sparkConf = new SparkConf().setMaster(master).setAppName("PopularMoviesNicer")
  
  val sc = new SparkContext(sparkConf)
  
  var nameDict = sc.broadcast(loadMovieNames)
  
println("Key value 20"+ nameDict.value(20)) 
  
  val lines = sc.textFile(inputLoc+"/u.data")
  
  val movieIdMap = lines.map(x => (x.split("\t")(1).toInt, 1))
  
  val totMovie = movieIdMap.reduceByKey((x, y) => (x+y))
  
  val flipData = totMovie.map(x => (x._2, x._1))  
  
  val sortedData = flipData.sortByKey()
  
  val movieDataWithName = sortedData.map(x => (x._2, nameDict.value(x._2), x._1))
  
  println("Highes movies rated with name and movie Id and total number of times rates")
  
  movieDataWithName.collect().foreach(println)
  }
  
}