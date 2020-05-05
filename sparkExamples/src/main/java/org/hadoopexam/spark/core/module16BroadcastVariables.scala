package org.hadoopexam.spark.core

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object module16BroadcastVariables {
  
  /*
   * Usage Explanation
   * args(0) - local[1]
   * args(1) - testdata\hadoopexam\input\module16Broadcastvariables\employee.txt
   * args(2) - testdata\hadoopexam\input\module16Broadcastvariables\city.txt
   * 
   */
  
  def main(args : Array[String]) : Unit = {
    if(args.length < 3) {
      println("Usage [master] [processinginputFIlePath] [broadcastInputFilePath]")
//      System.exit(1)
      
    }

    val master =  args.length match {
      case x: Int if x > 0  => {
        args(0)
      }
      case _ => "local[1]"
    }
//    args(0)
    val procssingInputFIlePath = args.length  match {
      case x :Int if x > 1 => {
        args(1)
      }
      case _ => "testdata\\hadoopexam\\input\\module16Broadcastvariables\\employee.txt"
    }
//      args(1)

    val broadCastInputfilePath = args.length match {
      case x: Int if x > 2 => {
        args(2)
      }
      case _ => "testdata\\hadoopexam\\input\\module16Broadcastvariables\\city.txt"
    }
//      args(2)
    
    val sc = new SparkContext(master, "module16BroadCasrVariable", System.getenv("SPARK_HOME"))
    Logger.getLogger("org").setLevel(Level.ERROR)
    val procssingDataRdd = sc.textFile(procssingInputFIlePath)
    
    val procssingData = procssingDataRdd.map(x => (x.split(",")(1), x.split(",")(2)))

    println("Procssing Data are ")
    procssingData.foreach(println)
    
    val broadcastDataRdd = sc.textFile(broadCastInputfilePath)
    
    val broadcastData = broadcastDataRdd.map(x => (x.split(",")(0) , x.split(",")(1)))

    println("BroadCast variables are")
    broadcastData.foreach(println)
    
    val bCities = sc.broadcast(broadcastData.collectAsMap())   
    
    //associating the data from broadCast data
    
    val result = procssingData.map(x => (x._1, x._2, bCities.value.getOrElse(x._2, -1)))
    

    println("Final data after associating with broadcast data is")
    result.foreach(println)
    
    
  }
  
  
}