package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source

object MovieSimilarities {
  
  def main(args: Array[String]):Unit = {
  
    
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
  }
  
  
  /** Load up a map of MovieId with movie names **/
  def loadMovieNames(inputLoc :String) : Map[Int, String] = {
       //Handle character encoding issues 
       implicit val codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  
  //Create a Map of String and populate it from u.item
      
      var moviesNames : Map[Int, String] = Map()
      
      val lines = Source.fromFile(inputLoc+"/u.item").getLines()
      
      for(line <- lines) {
        var fields = line.split("|")
        if(fields.length > 0) {
          moviesNames += (fields(0).toInt -> fields(1))
        }
        
      }
      return moviesNames
  }
  
  // defining type  Movie Id and Movie Rating
  type MovieRatings = (Int, Double)
  
  //Defining type userId with two MovieRatings Type 
  type UserRatingsPair = (Int, (MovieRatings, MovieRatings))
  
  //Takes argument as userRating and creates a pair of movieId and rating  
  def makePairs(userRatings: UserRatingsPair) =  {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    
    val movieId1 = movieRating1._1
    val movieId2 = movieRating2._1
    
    val rating1 = movieRating1._2
    val rating2 = movieRating2._2
    
   ( (movieId1, movieId2) , (rating1, rating2))
  }

  
  //filter movie

  def filterDuplicates(userRatingpair:UserRatingsPair):Boolean = {
    
    val movieRating1 = userRatingpair._2._1
    val movieRating2 = userRatingpair._2._1
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    return movie1 < movie2
  }

}