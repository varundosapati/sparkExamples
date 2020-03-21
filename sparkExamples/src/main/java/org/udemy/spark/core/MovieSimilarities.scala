package org.udemy.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import scala.math.sqrt


/*
 * 
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/RatingCounter
 * args(2) - testdata/udemy/spark/core/output/MovieSimilarities 
 * args(3) - 50
 * 
 * For running on command line 
 * 
 * Running on windows machine 
 *  Create input folder and place u.data, u.item
 *  Create output folder
 * 
 * spark-submit  --class org.udemy.spark.core.MovieSimilarities sparkExamples-0.0.1-SNAPSHOT.jar local[1] input output 50 
 * 
 * Running on unix machine 
 * 
 * Running on Hadoop cluster 
 */

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
    
    val outputLoc = args.length match {
      case z if z > 2 => args(2)
      case _ => "testdata/udemy/spark/core/output/MovieSimilarities"
    }
    
    val movieId:Int = args.length match {
      case q if q > 3 => args(3).toInt
      case _ => 50
    }
  
  val sparkConf = new SparkConf().setMaster(master).setAppName("PopularMoviesNicer")
  
  val sc = new SparkContext(sparkConf)

  //Read data from u.data file 
  
   println("\nLoading movie names...")
   val nameDict = loadMovieNames(inputLoc)
  
  val data = sc.textFile(inputLoc+"/u.data")
  
  //Map data into key/value pair (userId -> (movieId, rating))
  
  val ratings = data.map(x => (x.split("\t"))).map(x => (x(0).toInt ,  (x(1).toInt, x(2).toDouble )))
  
//  ratings.foreach(println)
  println("ratings count is "+ratings.count)
  //Emit every movie rated together by the same user 
  //Join the data so we get every possible movie/rating pair of every user watched
  
  val joinedData = ratings.join(ratings)
  println("joinedData count is "+joinedData.count)
//  joinedData.foreach(println)
  //At this point we have RDD consists of (userId => (movieID1, rating1), (movieID2, rating2))
  
  //Filter out duplicates 
  
  val uniqueRatings = joinedData.filter(filterDuplicates)
  
  println("uniqueRatings count is "+uniqueRatings.count)
  //now make key by (movie1, movie2) pairs
  
  
  val moviePairs = uniqueRatings.map(makePairs)
  println("moviePairs count is "+moviePairs.count)
  
  //Now we have (movie1, movie2) (rating1, rating2)
  //Now collect all ratings and compute similarities 
  
  val moviePairRating = moviePairs.groupByKey()
  println("moviePairRating count is "+moviePairRating.count)
  //Now we have (movie1, movie2) -> (rating1, rating2), (rating1, rating2) ....
  //can now compute similarities 
  
  val moviePairsimilarities = moviePairRating.mapValues(computeCosineSimilarities).cache()
  println("moviePairsimilarities count is "+moviePairsimilarities.count)
  
  //save the file if desired 
  
  val sorted= moviePairsimilarities.sortByKey()
  sorted.saveAsTextFile(outputLoc+"/movie-sim")
  
  /*
   * Extract similarities movie we care about that are good 
   * 
   * So we are assuming scorethreshould greater than 0.97 anbd coOccurenceThreshold greater than 50.0 
   * 
   * for certain movieId
   * 
   */
  
  val scoreThreshold = 0.97
  val coOccurenceThreshold = 50.0
  
  val filteredResults = moviePairsimilarities.filter(x => 
    {  
      val pair = x._1
      val sim = x._2
      (pair._1 == movieId || pair._2 == movieId) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold 
    }
  )
  

  
  //Now sort filteredResults by quality zero 
  
  val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
  
   println("\nTop 10 similar movies for " + nameDict(movieId))
   
   for(result <- results) {
     val sim = result._1
     val pair = result._2
     // Display the similarity result that isn't the movie we're looking at
     
     var similarMovieId = pair._1
     
     if(similarMovieId == movieId) {
       similarMovieId = pair._2
     }
     println(nameDict(similarMovieId) +"\t score : "+sim._1 +" \t strength "+sim._2)
     
   }
  
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
        var fields = line.split('|')
        if(fields.length > 0) {
          moviesNames += (fields(0).toInt -> fields(1))
         }
        
      }
       moviesNames.foreach(println)
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
    val movieRating2 = userRatingpair._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
//    println("movie 1 "+movie1+"and movie 2 "+movie2)
    return movie1 < movie2
  }
  
  
  //Create the RatingParis type 
  
  type RatingPair = (Double, Double)
  
  type RatingPairs = Iterable[RatingPair]

  //Create a function for computing consie similarities in ratings 
  
  def computeCosineSimilarities(ratingPairs: RatingPairs) : (Double, Int) = {
    
    var numPairs : Int = 0
    var sumXX : Double = 0.0
    var sumYY : Double = 0.0
    var sumXY : Double = 0.0
    
    for(pair <- ratingPairs) {
      
      val ratingX = pair._1
      val ratingY = pair._2
      
      sumXX = ratingX * ratingX
      sumYY = ratingY * ratingY
      sumXY = ratingX * ratingY
      numPairs +=1
    }
    
    val numarator : Double = sumXY 
    val denominator : Double = sqrt(sumXX) * sqrt(sumYY)
    
    var score:Double = 0.0
    
    if(denominator != 0) {
      score = numarator/denominator
    }
    
    return (score, numPairs)
  }
  
}