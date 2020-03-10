package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

/*
 * In this module we will go over CSV processing using dataset by adding window function for getting a sliding of data 
* 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module21CSVProcessing\
 * args(2) - testdata\hadoopexam\sparkSql\output\module21CSVProcessing\
 *
 */
object module32CSVProcessing {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
 
   case class MonthlySales(ID: String, sales: Double, month : Int) 
   
  def main(args : Array[String]) : Unit = {
    
    if(args.length <3 ) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module21CSVProcessing")
    
    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    
    val monthlySales = sparkSession.read.format("csv").options(Map({"header"->"true"}, {"inferSchema"-> "true"})).load(inputLoc+"MonthlySales.csv")
    
    
    /*
     * Prepare WindowSpec to cretae a 3 month sliding window for a product 
     * Negative subscript denotes rows above current row
     */
    
    val threeMonthWindow = Window.partitionBy(monthlySales("product")).orderBy(monthlySales("month")).rangeBetween(-2, 0)
    
    /*
     * Define compute on the sliding window, a moving average in this case 
     */
    val f = avg(monthlySales("sales")).over(threeMonthWindow)
    
    /*
     * Apply the sliding window and compute. Examine the results 
     */
     monthlySales.select($"product", $"sales", $"month", bround(f, 2).alias("Moving Avg")).orderBy($"product", $"month").show()
    

   }
}