package org.hadoopexam.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object module7Transformations {
  
  def main(args: Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sparkConf = new SparkConf().setAppName("module7").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    
    //Example of Map 
    val input = sc.parallelize(List(1, 2, 3), 3)
    val result = input.map(x => x*x)
    println("Example of Map")
    println(result.collect().mkString("|"))
    
    
    
    //Example of flatMap 
    
    val lines = sc.parallelize(List("hello hadoopExam.com", "hi"))
    
    val flatMapResult = lines.flatMap(line => line.split(" "))
    
    
    println("print all the info from flatMap"+flatMapResult.foreach(println))
    
    //Example of filter 
    println("Example of Filter")
    val resultFilter = input.filter(x => x!=1)
    println(resultFilter.collect().mkString("-"))
    
    
    //Another example of Map 
    println("Another Example of Map")
    val l = sc.parallelize(List(1, 2, 3,4 ,5))
    val lresult = l.map(x => x*2)
    println(lresult.collect().mkString("]"))
    
    println("Example of Implicit And Explicit UDF functions")
    //implicit function
    def getThanTwo(x:Int) = if(x> 2) Some(x) else None
    //explicit function
    def gtThanTwo: (Int) => Int = (x) => if(x > 2) x else 0 
    
    val getresultThan2 = lresult.map(x => getThanTwo(x))
    
    println("results greater than 2 "+getresultThan2.collect().mkString("["))
    
    //implicit function
    def multiPro(x:Int) = List(x-1, x, x+1)
    
    //Explicit function 
    
    def multiExppro :(Int) => List[Int] = (x) => List(x-1, x, x+1)
    val multiProResultMap = l.map(x => multiExppro(x))
    
    println("Getting single elemnts to get list of results using map"+multiProResultMap.collect().foreach(println))
    
    val flatResultMap = l.flatMap(x => multiExppro(x))
    
    println("Getting single elemnts to get list of results using map"+flatResultMap.collect().mkString(","))
        
    
    //Set operations 
    
    val rdd = sc.parallelize(List(1, 2, 3))
    
    val otherRdd = sc.parallelize(List(3, 4, 5))
  
    val resultRdd = rdd.union(otherRdd)
    
    println("Union of two rdd are "+resultRdd.collect().mkString(","))
    
    val resultIntersectionRdd = rdd.intersection(otherRdd)
     
    println("Intersection of two rdd are "+resultIntersectionRdd.collect().mkString(","))
    
    val subsctractRdd = rdd.subtract(otherRdd)
    
    println("Substract of one rdd to another rdd"+subsctractRdd.collect().mkString(","))
    
    val cartesianRdd = rdd.cartesian(otherRdd)

    println("Cartesian of two rdd are "+cartesianRdd.collect().mkString(","))
    
    
    println("Example of Keys")
    val lenAndKey = flatMapResult.map(x => (x.length(), x))

    println("Printing just the keys ")
    lenAndKey.keys.foreach(println)
    
    /*
     * Paired Transformations
     */
    println("Example of Paired Transformations")
  println("Example of group by operation")
  val ga = sc.parallelize(List("black", "blue", "white", "green", "grey"), 2)
  val gb = ga.keyBy(_.length())
    
  println(gb.groupByKey().collect().mkString("|"))
   
  println("Example of Join Operation ")
    
    
  }
  
  
}