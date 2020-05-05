package org.hadooexam.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File
import org.apache.spark.sql.SparkSession

/*
   * Usage Explanation
   * args(0) - local[1]
   * 
   * Need to work on this more 
   */



object module21HiveDataFrameExample {
  
  def main(args : Array[String]) : Unit = {
    
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local[1]"
    } 
    
//    val sc = new SparkContext(master, "module21HiveDataFrameExample", System.getenv("SPARK_HOME"))
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val warehouseLocatoin = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder()
                .master("local[*]")
                .appName("module21HiveDataFrameExample")
                .config("spark.sql.warehouse.dir", warehouseLocatoin)
                .config("spark.sql.catalogImplementation","hive")
                 .enableHiveSupport()
                 .getOrCreate()
    
    import spark.implicits._
    import spark.sql
    
    sql("create table if not exists  SparkHadoopEmployee(first_name string, last_name string, age int) row format delimited fields terminated by ','")
    
                 
    /**
     * 
     * Facing proble when using hiveContext with present version  - So trying to work with SParkSession 
     * Error is 
     * Exception in thread "main" org.apache.spark.sql.AnalysisException: org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:file:/C:/Users/swathi%20varun/git/sparkExamples/sparkExamples/spark-warehouse/sparkhadoopemployee is not a directory or unable to create one);
	at org.apache.spark.sql.hive.HiveExternalCatalog.withClient(HiveExternalCatalog.scala:107)
    val hiveContent = new HiveContext(sc)
    import hiveContent._
    
    /*
     * For hive we can create internally /user/hive/warehouse
     * We can also use expternal for creating teh table
     */
    
    hiveContent.sql("create table if not exists  SparkHadoopEmployee(first_name string, last_name string, age int) row format delimited fields terminated by ','")
    
    **/
    
    
    
    
    
  }
  
}