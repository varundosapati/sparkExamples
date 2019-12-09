package org.hadooexam.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/*
   * Usage Explanation
   * args(0) - local[1]
   * 
   * 
   */



object module21HiveDataFrameExample {
  
  def main(args : Array[String]) : Unit = {
    
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local[1]"
    } 
    
    val sc = new SparkContext(master, "module21HiveDataFrameExample", System.getenv("SPARK_HOME"))
    
    val hiveContent = new HiveContext(sc)
    
    hiveContent.sql("create table if not exists  SparkHadoopEmployee(first_name string, last_name string, age int) row format delimited fields terminated by ','")
    
    
    
    
    
    
    
  }
  
}