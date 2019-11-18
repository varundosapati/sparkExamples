package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Nil

object BasicIntersectByKey {
  
  
  /*
   * Explanantion 
   * 
   * ClassTags are concepts of reflections used to define for specific type instead of sending genericType
   * 
   * coGroup usage is to group all records 
   * Example 
   * 	rdd1 =  List((1, "panda"), (2, "Happy"))
   * 	rdd2 =  List((2, "panda"))
   * 	
   * 	rdd3 = (1,(CompactBuffer(panda),CompactBuffer())) 
   * 				 (2,(CompactBuffer(Happy),CompactBuffer(Pandas)))
   * 
   *
   *  
   */
  
  
  def intersectBy[T:ClassTag, V:ClassTag](rdd: RDD[(T, V)], rdd1 :RDD[(T, V)] ) : RDD[(T, V)] = {
    
    val rdd2 =rdd.cogroup(rdd1)
    rdd2.foreach(println)
    
  val rdd3 =    rdd.cogroup(rdd1).flatMapValues{ 
        case (Nil, _) => None
        case(_, Nil) => None
        case(x, y) =>  {
          /*
           * We get compactBuffer records of x value CompactBuffer(Happy)y value is CompactBuffer(Pandas)
           *
           * 
           */
          println("x value "+x +"y value is "+y)
          /*
           * Below are we using ++ operator to get all the An iterator returning all elements returned by iterator x, followed by all elements returned by iterator y 
           */
          
          x ++ y 
          }
    }
    
    rdd3.foreach(println)
    
    rdd3
  }
  
  
  def main(args: Array[String]) :Unit = {
    
    val master = args.length match {
      case x : Int if x> 0 => args(0)
      case _ => "local"
    }
    
    
    val sc = new SparkContext(master, "BasicIntersectByKey", System.getenv("SPARK_HOME"))
    
    val rdd1 = sc.parallelize(List((1, "panda"), (2, "Happy")))
    
    val rdd2 = sc.parallelize(List((2, "Pandas")))
    
    val irdd= intersectBy(rdd1, rdd2)
    
    irdd.foreach(println)
    
    val panda :List[(Int, String)] = irdd.collect().toList;
    
    panda.map(println(_))
    
    sc.stop()
    
  }
  
  
}