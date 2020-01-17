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
  
  val ja = sc.parallelize(List("blue", "green", "orange", "cucumber"), 3)
  val jb = ja.keyBy(_.length())
  println("First pair "+jb.collect().mkString(":"))
  val jc = sc.parallelize(List("black", "white", "grey"), 3)
  val jd = jc.keyBy(_.length())
  println("Second pair "+jd.collect().mkString(":"))
  
  println(jb.join(jd).collect().mkString(":"))
  
  println("Example of left Outer join")
  println(jb.leftOuterJoin(jd).collect().mkString(":"))
  
  println("Exmaple of Rightt outer Join")
  println(jb.rightOuterJoin(jd).collect().mkString(":"))
  
  println("Example of full outer join")
  println(jb.fullOuterJoin(jd).collect().mkString(":"))
  
  println()
  println(" Example of reduceByKey operation ")
  val ra = sc.parallelize(List("black", "blue", "white","green", "grey"), 3)
//  val rb = ra.keyBy(_.length())
  val rb = ra.map(x => (x.length(), x))
  println(rb.reduceByKey(_+_).collect().mkString("|"))

  val ra1 = sc.parallelize(List("black", "blue", "white", "orange"), 2)
  val rb1 = ra1.map(x => (x.length(), x))
  println(rb1.reduceByKey(_+_).collect().mkString("|"))
  
   println()
   println("Example of aggregate operation")
 
   val ax = sc.parallelize(List(1, 2, 7, 4, 30, 6), 3)
   println("Aggregate with 0 of 1, 2, 7, 4, 30, 6 with 3 partitions and max function"+ax.aggregate(0)(math.max(_, _), _+_))
  
   val ay = sc.parallelize(List("a", "b", "c", "d"), 2)
   println("Aggregate with initial value z of a, b, c, d with 2 partitions "+ay.aggregate("z")(_+_, _+_))
  
   val az = sc.parallelize(List("12", "234", "4563", "23"), 2)
   println("Aggregate of using math with min length "+ az.aggregate("")((x, y) => math.min(x.length(), y.length()).toString(), _+_))  
  
   println("Aggregate of using math with max length "+az.aggregate(" ")((x, y) => math.max(x.length(), y.length()).toString(), _+_))
   
  }
  
  
}