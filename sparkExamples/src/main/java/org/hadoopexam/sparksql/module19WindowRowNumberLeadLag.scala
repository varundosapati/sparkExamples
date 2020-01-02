package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * USAGE  - We are going to SparkSql Rank and Cummulative 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module19WindowRowNumberLeadLag\
 * args(2) - testdata\hadoopexam\sparkSql\output\module19WindowRowNumberLeadLag\
 */


object module19WindowRowNumberLeadLag {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class Employee(ID:Int, Name:String, gender:String, salary:Int, Department:String)
  
  def main(args: Array[String]) :Unit = {
    
    if(args.length < 3) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module19WindowRowNumberLeadLag")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      import sparkSession.implicits._
    
    val employeeDS = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance") , Employee(15, "Rajkumar", "Male", 4500, "Finance") , Employee(15, "Satish", "Male", 4500, "Finance") , Employee(15, "Himmat", "Male", 3500, "Finance"))).toDS() 
  
    
    val genderPartitionedSpec = Window.partitionBy("gender").orderBy($"salary".desc_nulls_last)
    println("USING WINDOW FUNCTION TO PARTITION BY GENDER AND ASCENDING SALARY TO PROVIDE ROW NUMBER ")
    employeeDS.withColumn("rowNumber", row_number over genderPartitionedSpec).show()
    
    /*
     * Select various ntile (Various Percentile)
     * if we divide salary in 3 qurtile then in which quartile is a fail 
     */
     println("DEVIDE CHECK TO SEE SALARY IN 3 TILE ALL SALARY RANGE ARE APPLIED  GENDER")
    employeeDS.select('*, ntile(3) over genderPartitionedSpec as "ntile").show()
    
    println("DEVEIDE CHECK TO SEE SALARY IN 4 TILE ALL SALARY RANGE ARE APPLIED FOR GENDER")
    employeeDS.select('*, ntile(4) over genderPartitionedSpec as "ntile").show()
    
    /**
     * Lead lag function
     */
    println("USING LAG FUNCTION TO GET THE PREVIOUS SALARY OF WINDOW OF PARTITION BY GENDER")
    employeeDS.withColumn("previousColumn", lag('salary, 1) over genderPartitionedSpec).show()
    
    println("USING LAG FUNCTION TO GET PREVIOUS OF PREVIOUS SALARY OF WINDOW OF PARTITION BY GENDER")
    employeeDS.withColumn("previousColumn", lag('salary, 2) over genderPartitionedSpec).show()
    
    
    println("USING LAG FUNCTION TO GET PREVIOUS OF PREVIOUS OF PREVIOUS SALARY OF WINDOW OF PARTITION BY GENDER")
    employeeDS.withColumn("previousColumn", lag('salary, 3) over genderPartitionedSpec).show()
    
  //Now try to get difference of previous value 
    println("USING LAG FUNCTION TO GET DIFFERENCE OF PREVIOUS SALARY ")
    employeeDS.withColumn("previousSalary", lag('salary, 1) over genderPartitionedSpec).select('ID, 'Name, 'gender, 'Department, 'salary, 'previousSalary, ('salary - 'previousSalary) as "difference").show()

    /*
     * Now lets see about lead function
     * Lead is opposite of lag which get the next value
     */

    println("USING LEAD TO GET THE NEXT VALUE ")
    employeeDS.withColumn("lead", lead('salary, 1) over genderPartitionedSpec).show()
    
    println("USING LEADING SALARY BY 2 TO GET THE NEXT NEXT VALUE ")
    employeeDS.withColumn("lead", lead('salary,2) over genderPartitionedSpec).show()
  
  }
  
  
}