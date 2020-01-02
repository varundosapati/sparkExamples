package org.hadoopexam.sparksql

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
 * USAGE  - We are going to SparkSql Rank and Cummulative 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module18SparkSQLRankCummulative\
 * args(2) - testdata\hadoopexam\sparkSql\output\module18SparkSQLRankCummulative\
 */
object module18SparkSQLRankCummulative {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class Employee(ID: Int, Name:String, gender:String, salary:Int, Department:String)
  
  def main(args :Array[String]) : Unit = {
    
    if(args.length < 3) {
      print("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module18SparkSQLRankCummulative")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    
    println("List of function in sparkSession catalog "+sparkSession.catalog.listFunctions().count())
    
    val employeeDs = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance") , Employee(15, "Rajkumar", "Male", 4500, "Finance") , Employee(15, "Satish", "Male", 4500, "Finance") , Employee(15, "Himmat", "Male", 3500, "Finance"))).toDS() 
    
    //Creating Window based on gender to rank their salary 
    //For the same salary it will give same rank in different genders 
    
    
    val genderPartitionedSpec = Window.partitionBy("gender").orderBy($"salary".asc_nulls_last)
    
    println("Applying rank salary by gender ")
    employeeDs.withColumn("rank", rank over genderPartitionedSpec).show()
   
    //Creating window based on department to rank the salary
    val departmentPartitionSpec = Window.partitionBy("Department").orderBy($"salary".asc_nulls_last)
    println("Applying rank salary by department ")
    employeeDs.withColumn("rank", rank over departmentPartitionSpec)
    
    //Creating window based on department and gender to rank the salary 
    val genderDepartmentPartitionSpec = Window.partitionBy("Department", "gender").orderBy($"salary".asc_nulls_last)
    println("Applying rank salary based on department and gender")
    employeeDs.withColumn("rank", rank over genderDepartmentPartitionSpec).show()
    
    //Lets get the percentage rank 
    println("Applying percent_rank salary based on gender ")
    employeeDs.withColumn("rank", percent_rank over genderPartitionedSpec).show()
    
    //Use the dense_rank for get sequence of rank by not missing any rank 
    println("Applying dense_rank salary based on gender ")
    employeeDs.withColumn("rank", dense_rank over genderPartitionedSpec)
    
    
    
    /*
     * Cummulative Distribution 
     * 
     */
    println("Applying cume_dist by salary based on gender")
    employeeDs.withColumn("rank", cume_dist over genderPartitionedSpec).show()
    
    println("Applying cume_dist by salary based on gender department")
    employeeDs.withColumn("rank", cume_dist over genderDepartmentPartitionSpec).show()
    
    
    
    
  }
  
}