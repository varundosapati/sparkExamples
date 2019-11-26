package org.hadoopexam.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * reduce(_+_) : accepts two elements and result as the same type
 * fold(initialVla)(_+_) : accepts two elements with initial value and result as the same type
 * Usage of Fold over reduce - When we get an empty collection reduce throw exception instead if we use fold it throws initial value
 *  
 * aggregate() : accepts two function input aggregate can have different result type
 * 
 * accumalator : As in hadoop world as counter , accumalator gets (writes)incremented in each worker and cannot read each other accumalator, and driver only reads the accumalator and can get final sum 
 * Usage of accumalator - Used for dbugging purpose, long running jobs , can be most used effective for counter
 * 
 * 
 */

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
  
  /*
   * Example of using fold action on integer 
   */
  
  val foldReq = sc.parallelize(List(1, 2, 3, 4), 2)
  
  val foldRes = foldReq.fold(1)(_ + _)
  
  println("Result of fold initial value for each partition "+foldRes)
  
   /*
    * Example of using fold on string 
    */
  
  val foldreqStr = sc.parallelize(List("a", "b", "c", "d"), 3)
  
  val foldResStr = foldreqStr.fold("z")(_+_)
  
  println("Result of fold initial value z and three partitions "+foldResStr)
  
  }
  
  
  
}
