package org.hadoopexam.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object module8 {

  
  def main(args: Array[String]):Unit = {
    
    val sparkConf = new SparkConf().setAppName("module8").setMaster("local[*]")
    
    val sc = new SparkContext(sparkConf)
    
    
    
    //Creating an rdd from 1 to 100 and partitioning to 3
    val a = sc.parallelize(1 to 100, 2)
    //reduce the content
   var result = a.reduce(_+_)
    
   println("Reduce of 1 to 100 is "+result)
    
   result = a.fold(1)(_+_)
   
   println("fold of 1 to 100 is "+result)
   
   
   // reduce numbers 1 to 10 by adding them up
  val x = sc.parallelize(1 to 10, 2)
  val y = x.reduce((accum,n) => (accum + n)) 
  // y: Int = 55
  println("x data is "+y)
   
// shorter syntax
//val y = x.reduce(_ + _) 
// y: Int = 55
 
// same thing for multiplication
//val y = x.reduce(_ * _) 
// y: Int = 3628800
   
  }
  
}
