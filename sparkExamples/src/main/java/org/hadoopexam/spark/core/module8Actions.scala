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

object module8Actions {

  
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
  

    val counter = sc.accumulator(10, "testAcc")
    
    /*
   * Aggregator - Accepts two functions and returns different value, IN aggregate we also have zero value of type we want to return 
   *  
   *  first function - to combine values locally on each partition with accumalator
   *  
   *  second function - merge two accumalators [reduce all accumalators]
   *  
   * Examples - Accepts string and return a double value 
   */
 
    val inputAgg = sc.parallelize(List(1, 2, 3, 4, 5))

//    inputAgg.aggregate(zeroValue)(seqOp, combOp)
    
    /*
     * In below example (0,0) as initial value for summing values and summing accumalators
     * 
     * the (accu, value) - accu is pointed to the (0,0) and value is pointed to our RDD
     * 
     * So when data loops thrpugh accu._1 get the first zero value and adds with value
     * and then accu.2 get the second zero value and adding with 1 here 
     * 
     * Now (x, y) - x is sum of values with initial value in each partition 
     * 						- y is sum of accumalators with inital value in each partition
     * 
     * We finally making the (x._1+y._1, x._2+y._2) combine operation on driver  
     * 
     */
    
    val inputresultAgg = inputAgg.aggregate(0,0) ( (accu, value) => (accu._1+value , accu._2+1) , (x, y) => (x._1+y._1, x._2+y._2) )
    
    println("resulted aggregate of inputAgg"+inputresultAgg)
  
    val avgResult = inputresultAgg._1/inputresultAgg._2
    
    println("avg result is "+avgResult)
  
    
    /*
     * Aggregate another example
     *      
     */
    
  val nextInput = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
  
  nextInput.mapPartitionsWithIndex(myIntPartitionFun).foreach(println)
    
 println("Sum of maximum elemnts "+nextInput.aggregate(0)(Math.max(_, _) , _+_))     
  
 /*
  * Explanation  about below - Three partitions (1, 2)( 3, 4) ( 5,6)
  * 
  * As we are sending 5 as initial value (5, 1, 2) (5, 3, 4) (5, 5, 6)
  * 
  * So Max of each partition is (5)(5)(6)
  * 
  * And sum of this values in driver progam is 5 + 5 + 5 + 6
  * 
  * 
  */
 println("Sum of maximum elemnts with initial value 5  "+nextInput.aggregate(5)(Math.max(_, _) , _+_))
  
 
 /*
  * Another example for aggregate with string
  */
 
 val inputStr = sc.parallelize(List("a", "b", "c", "d", "e"), 3)
 
 inputStr.mapPartitionsWithIndex(myStringPartitionFun).foreach(println)
 
 println("Aggregating a inputStr "+inputStr.aggregate("")(_+_, _+_))
 
 
 println("Aggregating a z value for inputStr "+inputStr.aggregate("z")(_+_, _+_))
 
 
 
 /*
  * Another example for aggregate
  */
 
 
 val inputValStr = sc.parallelize(List("12", "23", "345", "456"), 3)
 
 val maxLenOutput = inputValStr.aggregate("")((x, y)=> (Math.max(x.length(), y.length())).toString(), (_+_))
 
 println("Result of output "+maxLenOutput)

 val minLenOutput = inputValStr.aggregate("")((x, y) => (Math.min(x.length(), y.length())).toString(), (_+_))
 
 println("Result of min length output "+minLenOutput)
 
 /*
   * Collect - which return entire rdd back to driver (driver memory needs to be capable to hold that much content)
   * 
   */
   
   inputAgg.collect()    
    /*
     * take - Return n number of elements from rdd, if RDD is partitioned gets minimum number of partitions
     * Order is not fixed 
     */
    
    /*
     * top() - It will be using default ordering on data 
     */
 
     
   /*
    * foreach() - performs action on each element of RDD, It work on each worker node independently, 
    * for each will not return data to driver node 
    */
  
   
   /*
    * count() - returns number of elements in RDD
    */
   
   /*
    * CountByValue() - Number of each element access in RDD 
    */
   
   val inputDuplicateReq = sc.parallelize(List(2, 3, 4,5 ,6 ,7, 1,8, 1, 1, 1, 1, 1, 1,2))
   
   println("count by values are ")
   inputDuplicateReq.countByValue().foreach(println)
   
  }
  
  def myStringPartitionFun(index : Int, iter:Iterator[String]) : Iterator[String] = {
    iter.toList.map(x => "partId "+ index+ ", val "+x).iterator
  }
  
  

  def myIntPartitionFun(index:Int, iter:Iterator[Int]) : Iterator[String] = {
    iter.toList.map(x => "partId"+ index + ", val "+ x).iterator
  }
  
  
  
  
}
