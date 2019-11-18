package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

/*
 * This example first creates List(1, 2, 3, 4) creates a Map by doubling the value List(1,4, 9, 16)
 * And filtering the data which are not equal 1 result are List(4, 9, 16)
 *  
 */

object BasicMapThenFilter {
  
  def main(args : Array[String]) : Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[1]"
    }
    
    val sc = new SparkContext(master, "BasicMapAndFilter", System.getenv("SPARK_HOME"))
    
    val input = sc.parallelize(List(1, 2, 3, 4))
    
    val squared = input.map(x => x*x)
    
    val filtered = squared.filter(x => x!=1)
    
    println("Filered data which are not equal 1 are "+filtered.collect().mkString("|"))
  
  }
  
}