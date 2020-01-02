package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 * USAGE  - We are going to create example of Rollup, pivot and cube using spark sql 
 * 
 * TODO: need to try to find function for dataset which are equivalent for rollup, pivot, cube
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module16GroupByRollupPivotCube\
 * args(2) - testdata\hadoopexam\sparkSql\output\module16GroupByRollupPivotCube\
 */

object module16GroupByRollupPivotCube {
  Logger.getLogger("org").setLevel(Level.ERROR) 
  
  case class Employee(ID:Int, Name: String, gender:String, Salary:Int, Department:String )
  
  def main(args : Array[String]) : Unit = {
    
    if(args.length < 3) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module16GroupByRollupPivotCube")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    
    import sparkSession.implicits._
    
    val DS = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance"))).toDS()    
    
    println("Display records in dataset")
    DS.show()
    println("Schema of Dataset")
    DS.printSchema()
    
    DS.createOrReplaceTempView("EMPLOYEE")
    
    println("Select data from created EMPLOYEE table")
    
    sparkSession.sql("SELECT * FROM EMPLOYEE").show()
    
    println("Grouping by department ")
    sparkSession.sql("SELECT department, sum(salary) FROM EMPLOYEE group by department").show()
    
    /*
     * Example of ROLLUP
     */
    
    println("Example of rollup")
    println("Grouping of department salaries and total salaries ")
    sparkSession.sql("SELECT coalesce(Department,'All Departments') Departments , sum(salary) as Salary_Sum FROM EMPLOYEE GROUP BY ROLLUP (Department)").show()
  
    println("Example of group by with rollup with multiple columns")
    println("Grouping of department and gender salaries and also total salaries")
    sparkSession.sql("SELECT coalesce(Department,'All Departments') Department , sum(salary) as Salary_Sum, coalesce(gender, 'All Gender') as Gender FROM EMPLOYEE GROUP BY ROLLUP (Department, Gender)").orderBy("Department").show()
    
    /*
     * Exmaple of CUBE
     */
    println("Example of Group by cube with multiple columns")
    println("Grouping Cube by department and gender salaries and also total salaries")
    sparkSession.sql("SELECT coalesce(Department,'All Departments') Department , sum(salary) as Salary_Sum, coalesce(gender, 'All Gender') as Gender FROM EMPLOYEE GROUP BY CUBE (Department, Gender)").orderBy("Department").show()
    
    /*
     * Example of PIVOT with dataframe 
     */    
     val courseStudents = Seq( ("Hadoop", 13000,2011), ("Spark", 11000,2013), ("Cassandra", 12000,2015), ("Java", 19000,2010), ("Python", 13000,2009), ("SQL", 24000,2009), ("Scala", 34000,2013), ("Hadoop", 12000,2012), ("Spark", 12000,2014), ("Cassandra", 11000,2016), ("Hadoop", 13000,2011), ("Spark", 11000,2013), ("Cassandra", 12000,2015), ("Java", 19000,2010), ("Python", 13000,2009), ("SQL", 24000,2009), ("Scala", 34000,2013), ("Hadoop", 12000,2012), ("Spark", 12000,2014), ("Cassandra", 11000,2016) ).toDF("name", "students" , "year") 
     println("Example of GROUP BY PIVOT with columns ")
    
     println("GETTING TOTDAL STUDENTS BY GROUP BY NAMES FOR EACH YEAR")
     val pivotedData = courseStudents.groupBy("name").pivot("year").sum("students")
     pivotedData.show()
     
     println("GETTING TOTAL STUDENTS BY GROUP BY NAME FOR SPECIFIC YEAR")
     val specificyearData = courseStudents.groupBy("name").pivot("year").sum("students")
     specificyearData.show()
     
     /*
      * Example of using caseClass for Dataset with PIVOT 
      */
     
     val DS1 = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance") , Employee(15, "Rajkumar", "Male", 4500, "Finance") , Employee(15, "Satish", "Male", 4500, "Finance") , Employee(15, "Himmat", "Male", 3500, "Finance"))).toDS() 
     println("USING DATASET FOR GETTING TOTAL SALARY BY GROUPING GENDER FOR EACH DEPARTMENT ")
     val pivotDataset = DS1.groupBy("gender").pivot("Department").sum("Salary")
     pivotDataset.show()
  
     
     /*
      * Example of using caseClass with dataset for API fro Rollup and Cube 
      */
     
     //Create Sample Dataset 
     val DS2 = sparkSession.sparkContext.parallelize(Seq( Employee(1, "Deva", "Male", 5000, "Sales"), Employee(2, "Jugnu", "Female", 6000, "HR"), Employee(3, "Kavita", "Female", 7500, "IT"), Employee(4, "Vikram", "Male", 6500, "Marketing"), Employee(5, "Shabana", "Female", 5500, "Finance"), Employee(6, "Shantilal", "Male", 8000, "Sales"), Employee(7, "Vinod", "Male", 7200, "HR"), Employee(8, "Vimla", "Female", 6600, "IT"), Employee(9, "Jasmin", "Female", 5400, "Marketing"), Employee(10, "Lovely", "Female", 6300, "Finance"), Employee(11, "Mohan", "Male", 5700, "Sales"), Employee(12, "Purvish", "Male", 7000, "HR"), Employee(13, "Jinat", "Female", 7100, "IT"), Employee(14, "Eva", "Female", 6800,"Marketing"), Employee(15, "Jitendra", "Male", 5000, "Finance"))).toDS() 
 
     //Apply Rollup, so that we can do the total and subtotal
     
     println("GROUP BY ROLLUP FOR EACH DEPARTMENT SALARY AND GET TOTAL SALARY FOR ALL DEPARTMENTS ")
     DS2.rollup($"Department").agg(sum("Salary")).sort($"Department".asc_nulls_last).show()
     
     //Apply rollUp On Depatrment and Gender and also change value of null if in case for total 
     println("GROUP BY ROLLUP FOR EACH DEPARTMENT AND GENDER FOR SALARY AND GET TOTAL SALARY FOR ALL DEPARTMENTS AND IN CASE OF NULL VALUE FOR DEPARTMENT AS TOTAL SALARIES ")
     DS2.rollup($"Department", $"Gender").agg(sum("Salary") as "Salary").select(coalesce($"Department", lit("ALLDEPATRMENTS")) as "Department", coalesce($"Gender", lit("ALL GENDERS")) as "Gender", $"Salary").sort($"Department".asc_nulls_last, $"Gender".asc_nulls_last).show()
     
     //Apply cube operator on Department and Gender to get Grand total for Department and all Genders for the salaries column 
     println("GROUP BY CUBE FOR EACH DEPARTMENT AND GENDER FOR SALARIES WHICH GET TOTAL OF ALL DEPARTMENT AND GENDER AND GRAND TOTAL OF EACH")
     DS2.cube($"Department", $"Gender").agg(sum("Salary") as  "Salary").select(coalesce($"Department", lit("ALL Departments")) as "Department", coalesce($"Gender", lit("ALL GENDERS")) as "Gender", $"Salary").sort($"Department".asc_nulls_last, $"Gender".asc_nulls_last).show()
     
     
     
     
     
     
  }
  
  
}