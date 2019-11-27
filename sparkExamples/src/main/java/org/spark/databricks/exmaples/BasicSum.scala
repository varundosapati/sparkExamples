package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

object BasicSum {
  
  def main(args : Array[String]):Unit = {
    
    val master = args.length match {
      case x : Int if x > 0  =>  args(0)
      case _ => "local"
    }
    
    
    val sc = new SparkContext(master, "BasicSum", System.getenv("SPARK_HOME"))
    
    val input = sc.parallelize(List(1, 2, 3, 4))
    
    val sum = input.fold(0)(_+_)
    
    println("Sum of 1 to 4 with initial of 0 is "+sum)
  }
  
}