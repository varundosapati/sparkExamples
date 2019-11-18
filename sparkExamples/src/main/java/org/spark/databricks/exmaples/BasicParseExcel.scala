package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

object BasicParseExcel {
  /*
   * Explanantion : we are using crealytics (spark-excel) package to process data of excel into dataframes 
   * 
   * TODO : Need to format date value 
   * 
   */
  
  def main(args: Array[String]) :  Unit = {
    
    if(args.length < 3) {
      println("Input format [local] [inputFilePath] [outputFilePath]")
      System.exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "BasicParseExcel", System.getenv("SPARK_HOME"))
    
    val sqlContext = new SQLContext(sc)
    
    val data = sqlContext.read.
                        format("com.crealytics.spark.excel")
                        .option("useHeader", "true")
                        .option("sheetName", "Equity")
                        .option("timestampFormat", "yyyyMMdd")
                        .option("treatEmptyValuesAsNulls", "true")
                        .option("inferSchema", "true")
                        .option("addColorColumns", "false")
                        .load(inputFile)
    data.show(false)
    
    
    val movingAvgForeachTicker = data.foreach(x => {
    
      println("Ticker "+x(2)+" Moving avg "+x(11))
      
    })
    
  }
  
}