package org.udemy.spark.core

import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata/udemy/spark/core/input/MostPopularSuperHero
 */

object DegreeOfSerparation {
  
   Logger.getLogger("org").setLevel(Level.ERROR)

  //The character we want to find the separation between
  val startCharacterId = 5306 //5306 //SpiderMan
  val targetCharacterId = 3031//14 //ADAM 3,031

  //We make an accumalator a "global" Option so we can reference it in mapper later

  val hitCounter: Option[LongAccumulator] = None 
  
//  Some(LongAccumulator(value: 0))

  //Some custom data types
  //BFSData contains an array of heroId, distance ,
  type BFSData = (Array[Int], Int, String)

  type BFSNode = (Int, BFSData)

  /*
   * Convert a line of input data into BFSNode
   */

  def convertToBFS(line: String): BFSNode = {

    //Split up the line in fields
    val fields = line.split("\\s+")

    //Extract the hero Id from initial value

    val heroId = fields(0).toInt

    //Extract subsequent heroId  into the connections array

    var connections: ArrayBuffer[Int] = ArrayBuffer()

    for (connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    //Default Distance and color is 9999 and white
    var distance = 9999
    var color = "WHITE"

    //Unless this the character we're starting from

    if (heroId == startCharacterId) {
      color = "GRAY"
      distance = 0
    }
//    println("HeroId"+heroId+ " Connections "+connections.toArray)
    (heroId, (connections.toArray, distance, color))
  }

  /*
   * Load data using marvel-graph.txt
   * Creation of iteration 0 of Our RDD of BFSNode
   *
   */
  def createStartingRdd(sc: SparkContext, inputLoc: String): RDD[BFSNode] = {
    val data = sc.textFile(inputLoc + "/marvel-graph.txt")
    return data.map(convertToBFS)
  }

  /*
   * bfsMap expands BFSNode into this node and its children 
   * 
   * This method is called from flatMap , so we return an array of potentially BFSNode to add to our new RDD
   */
  
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    //Extract data from BFSNode
    val characterId = node._1
    val data:BFSData = node._2
    
    val connections: Array[Int] = data._1
    val distance:Int =  data._2
    var color:String = data._3
    
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()
    
    //Gray Nodes are flagged for expansion, and create new gray nodes for each connection 
    
    if(color == "GRAY") {
        for(connection <- connections) {
          val newCharacter = connection 
          val newDistance = distance+1
          val newColor = "GRAY"
          
          //Have we stumbled accross the character we are looking for 
          //if so we increment the accumulator so the dirver program knows 
          
          if(targetCharacterId == newCharacter) {
            println("found targetCharacterId"+hitCounter.isDefined)
            if(hitCounter.isDefined) {
               println("Happening hitCounter increment value")
                hitCounter.get.add(1)
            }
          }
          
          //Create our grey node for this connectoin and add it to results 
          val newEntry :BFSNode = (newCharacter, (Array(), newDistance, newColor))
          results += newEntry
        }
        //Color this node as black, indication it has been processed already 
        color = "BLACK"
    }
    
    //Add the original node back in, so its connections can get merged back with grey nodes in the reducer
    val thisBFSNode :BFSNode = (characterId, (connections, distance, color))
    results += thisBFSNode
    
    return results.toArray
  }

  
  
  /** Combine nodes for the same heroID, preserving the shortest length and darkesh color **/
  
  
  def bfsReduce(data1:BFSData, data2:BFSData) : BFSData = {
    
    //Extract the data we are combining 
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    
    val disatnce1:Int = data1._2
    val distance2:Int = data2._2
    
    val color1:String = data1._3
    val color2:String = data2._3
    
    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()
    
    //See if one is original node with its connection, If so preserve them 
    
    if(edges1.length > 0) {
      edges ++= edges1
    }
    
    if(edges2.length > 0) {
      edges ++=edges2
    }
  
    //preserve minimum distance 
    
    if(disatnce1 < distance) {
      distance = disatnce1
    }
    
    if(distance2 < distance) {
      distance = distance2
    }
    
    
    //Preserve darkest color 
  
    if(color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    
    if(color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if(color2 == "WHITE" && (color1 == "GRAY" || color1=="BLACK")) {
      color = color1
    }
    if(color2 =="GRAY" && color1=="BLACK") {
      color = color1
    }
    if(color1=="GRAY" && color2=="GRAY") {
      color = color1
    }
    if(color1=="BLACK" && color2=="BLACK"){
      color=color1
    }
    return (edges.toArray, distance, color)
  
  }
  
  def main(args: Array[String]) {
    
    
    if(args.length < 2) {
      println("USAGE MASTER INPUTLOC")
      System.exit(0)
    }
    
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local[1]"
    }
    
    val inputLoc = args.length match {
      case x if x > 1 => args(1)
      case _ => "testdata/udemy/spark/core/input/MostPopularSuperHero"
    }

    
    val sparkConf = new SparkConf().setAppName("DegreeOfSerparation").setMaster(master)
  
    val sc = new SparkContext(sparkConf)
    
    
    var iterationRdd = createStartingRdd(sc, inputLoc)
    
    var iterationCount:Int = 0
    
//   hitCounter = LongAccumulator(value: 0)
    
    for(iteration <- 1 to 20) {
      println("Running BFS Iteration #"+iteration)
      
      //Create new vertices as needed to darken or reduce distance in reduce stage 
      //If we encounter the node we are looking for we as GRAY node, we increment our accumaltor to signal we are done 
      
      val mapped = iterationRdd.flatMap(bfsMap)
      
      /*
       * Note that mapped.count() action here forces the RDD to be evaluated , and that's the only 
       * only reason our accumulator is only updated 
       */
      
       println("Processing " + mapped.count() + " values.")
       
        if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        println("HitCount value"+hitCount)
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + 
              " different direction(s).")
          return
        }
      }
      
      //Reducer combines data for each character ID, preserving the darkest color and shortest path
      iterationRdd = mapped.reduceByKey(bfsReduce)
      
    }
    
    
  }

}