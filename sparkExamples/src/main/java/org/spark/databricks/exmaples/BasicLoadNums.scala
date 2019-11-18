package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.orc.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.compress.DefaultCodec

object BasicLoadNums {
  
  /*
   * Paramater to be sent when running the program 
   * 
   * args(0) - local[1]
   * args(1) - testdata/databricks/BasicLoadNums.txt
   * args(2) - testdata/databricks/output/BasicLoadNumsOutput
   * 
   * */
  
  def main(args : Array[String]) : Unit = {
    
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "BasicLoadNums", System.getenv("SPARK_HOME"))
    
    
    val fileRdd = sc.textFile(inputFile)
    fileRdd.foreach(println)
    /*
     * Usage of accumulator is to increment happens at the driver instead of each executor (Inside a worker node)
     */
    val errorLines = sc.accumulator(0, "errorLines") 
    
    val datalines = sc.accumulator(0, "datalines")
    
    val splittedData = fileRdd.flatMap( line => {
      
      try{
          println("Happening")
          val input=  line.split(" ")
          
          println(input.mkString("|"))  
          
          val data = Some(input(1).toInt, input(0))
          datalines += 1
          data 
        } catch {
            case e : java.lang.NumberFormatException => {
              errorLines += 1
              None
            }   
            
            case e : java.lang.ArrayIndexOutOfBoundsException => {
              errorLines += 1
              None
            }
        } 
    }).reduceByKey(_+_)
    
    splittedData.foreach(println)
    
//    val counts = fileRdd.map( line => { 
//      try{
//          println("Happening 1")
//          val input = line.split(" ")
//          println("Splitted line is "+input.mkString("|"))
//          val data = Some(input(0), input(1).toInt)
//          datalines += 1
//          data
//        } catch {
//          case e : java.lang.NumberFormatException => {
//            errorLines += 1
//            None
//          }
//          case e : java.lang.ArrayIndexOutOfBoundsException => {
//            errorLines += 1
//            None
//          }
//      }
//        
//    })
//    println("Printing the counts ")
//    counts.foreach( x => {
//      
//      x.foreach(y => {
//        val name = y._1.toUpperCase()
//        println("Record value is"+name)
//      })
//    })
    
    
    if(errorLines.value > 0.1*datalines.value) {
//      splittedData.saveAsTextFile(outputFile)
      splittedData.saveAsSequenceFile(outputFile, Some(classOf[DefaultCodec]))
      
    } else {
      println(s"Too many errros ${errorLines.value} for ${datalines.value} ")
    }
    
  }
   
}