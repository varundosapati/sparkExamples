package org.spark.databricks.exmaples

import org.apache.spark.SparkContext

object BasicMap {
  
  /*
   * Example of BasicMap 
   * 
   * Created a List using sparkContext
   * 
   * Used Map on inputRdd to convert List(1, 2, 3, 4) -> List(1, 4, , 6, 16)
   */
  
  def main(args : Array[String]) :Unit = {
    
    val master = args.length match {
      case x: Int if x> 0 => {
        args(0)
      }
      case _ => "local[1]"
      
    }   
      val sc = new SparkContext(master, "BasicMap", System.getenv("SPARK_HOME"))
      
      val inputRdd = sc.parallelize(List(1, 2, 3, 4))
      
      val mapRdd = inputRdd.map(x => x*x )
      
      println("result in defaultMapRdd" + mapRdd.collect().mkString("|"))
      
        
  }
  
  
}