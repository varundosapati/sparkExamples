package org.spark.databricks.exmaples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BasicAvgWithKyro {
  
  def main(args: Array[String]) :Unit = {
    
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ =>  "local"
    }
    
    val conf = new SparkConf().setMaster(master).setAppName("BasicAvgWithKyro")
    
//    conf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
    
          conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    
    val sc = new SparkContext(conf)
    
    val input = sc.parallelize(List(1, 2, 3, 4))
    
    val result = input.aggregate((0, 0))((acc, value) => (acc._1+value, acc._2+1), (acc1, acc2) => (acc1._1+acc2._1 , acc1._2+acc2._2))

    val avg = result._1/result._2
    
    println("Avg result of List(1, 2, 3, 4) in one partition is"+avg)
    
    
  }
  
  
}