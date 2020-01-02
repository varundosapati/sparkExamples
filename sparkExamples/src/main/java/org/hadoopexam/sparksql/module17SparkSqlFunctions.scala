package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * USAGE  - We are going to create standard UDF and aggregate functions * 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module17SparkSqlFunctions\
 * args(2) - testdata\hadoopexam\sparkSql\output\module17SparkSqlFunctions\
 */

object module17SparkSqlFunctions {
 
   Logger.getLogger("org").setLevel(Level.ERROR)
  case class Employee(Id:Int, Name:String, gender : String, Salary:Int, Department :String)
  
  //deine the logic for creating your SPARK SQL aggregate function by extending class 
  
  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
  
    /*
     * Define input data schema as well as result schema 
     * input data will  as ID: interger, Name:String, gender :String, Salary:integer, Department:String
     */
    def inputSchema :StructType = {
      new StructType().add("ID", IntegerType).add("Name", StringType).add("gender", StringType).add("Salary", IntegerType).add("Department", StringType)
    }
    
    //Define buffer  scheema output = (Integer total)
    
    def bufferSchema : StructType = new StructType().add("total",IntegerType)
    def dataType : DataType = IntegerType
    
    //determistic UDAF output given an input is determistic 
    
    def deterministic : Boolean = true
    //Initialize the resut to 0 MutableAggregationBuffer represents a row object 
    def initialize(buffer :MutableAggregationBuffer) : Unit ={buffer.update(0, 0)}
    
    //Update the buffer
    def update(buffer : MutableAggregationBuffer, row : Row) : Unit = {
        //Intermediate result to update 
        val interMedateSum = buffer.getInt(0)
        
        //First input paramater 
        val salary = row.getInt(0)
        
        //Update Intermediate result 
        buffer.update(0, interMedateSum+(salary))
        
    }
    
    //merge intermediate results sum by adding them 
    
    def merge (buffer1 : MutableAggregationBuffer, buffer2 : Row) :Unit = {
      buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0))
    }
    
    //The result to be evaluated finally 
    
    def evaluate(buffer : Row) : Any = {
      buffer.getInt(0)
    }
  
  }
  
  
  
  
  def main(args:Array[String]) :Unit ={
    
    if(args.length < 3){
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module17SparkSqlFunctions")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      import sparkSession.implicits._
    
    val employeeDS = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance") , Employee(15, "Rajkumar", "Male", 4500, "Finance") , Employee(15, "Satish", "Male", 4500, "Finance") , Employee(15, "Himmat", "Male", 3500, "Finance"))).toDS() 
   
    employeeDS.show()
    
    /*
     * Explicitly Define UDF function which adds 20% of sal 
     * Remember for SparkSQL it is difficult to optimize UDF function 
     * Hence you should as less as possible
     */
    
    val calculateTotalSal = (sal:Int ) => {sal * 20/100}+sal
    
    
    val udfCalculateTotalSalWithBonus = udf(calculateTotalSal)
    
    //Now lets use the above udf function to display salary 
    
    println("Displaying salary by adding bonus to salary of employees")
    employeeDS.withColumn("Total Salary", udfCalculateTotalSalWithBonus($"Salary")).show()
    
    /*
     * Now lets create a InlineUdf and use on Dataset API 
     */
    
    //Creating inline UDF function using lambda function 
    val inlineUdfCalculateTotalSalWithBonus = udf( (sal:Int) => {sal * 20/100}+sal, IntegerType)
    
    println("Dispalying salary with boonus using Inline UDF function in Dataset API")
    employeeDS.withColumn("Total Salary", inlineUdfCalculateTotalSalWithBonus($"Salary")).show()
    
    /*
     * Lets register the Eplicityly defined UDF function to sparkSession 
     */
    sparkSession.udf.register("TotalSalaryWithBonus", udfCalculateTotalSalWithBonus)
     
    println("Lets check to see in catalog to see udf function TotalSalaryWithBonus exists ")
    
    sparkSession.catalog.listFunctions().filter('name like "%Bonus%").show(false)
 
    employeeDS.createOrReplaceTempView("EMPLOYEE")
    
    println("Applying registered UDF function on select statment of spark sql ")
    sparkSession.sql("SELECT ID, Name, gender, salary, Department, TotalSalaryWithBonus(salary) as TotalSalary FROM  Employee").show()
    
    
        
    /*
     * Lets see some exmaples on aggregate functions now 
     * We will register Aggregate function to sparkSession 
     * 
     */
    sparkSession.udf.register("TOTALAGGSALRY", new SumProductAggregateFunction)
   
    println("Using the AGGREGATE FUNCTION doing the total salary for each department ")
    sparkSession.sql("SELECT Department, TOTALAGGSALRY(Salary) FROM EMPLOYEE GROUP BY Department").show()
    
    sparkSession.sql("SELECT TOTALAGGSALRY(Salary) FROM EMPLOYEE").show()
    
    
  }
  
}