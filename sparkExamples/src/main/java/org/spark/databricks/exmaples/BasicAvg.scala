package org.spark.databricks.exmaples

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object BasicAvg {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4,5), 1)
    val result = computeAvg(input)
      
    val avg = result._1 / result._2.toFloat
    println("result"+result)
    println("avg"+avg)
    
    
    //Another examople just send single paramter in avg
    
    val result1Avg = input.aggregate(2)((acc, value) => (acc+value), (acc1,acc2) => (acc1+acc2))

    println("resultAvg"+result1Avg)
  }
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((1, 0, 1))((x, y) => (x._1 + y, x._2 + 1, x._3+1),
      (x,y) => (x._1 + y._1, x._2 + y._2, x._3+y._3))
  
  }
}