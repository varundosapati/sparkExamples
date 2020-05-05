package org.hadoopexam.spark.core

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


 /*
   * Usage Explanation
   * args(0) - local[1]
   * args(1) - testdata\hadoopexam\input\module18joinGroupBySwap\employeeName.csv
   * args(2) - testdata\hadoopexam\input\module18joinGroupBySwap\employeeSalary.csv
   * args(3) - testdata\hadoopexam\output\module18joinGroupBySwap\
   * 
   */
object module18JoinGroupBySwap {
  
  def main(args: Array[String]) : Unit = {
    
    if(args.length < 4) {
      println("USAGE [master]  [inputFIle1] [inputFile2] [outputFile]")
//      System.exit(1)
    }
    
    
    val master = args.length match {
      case x : Int if x > 0 => {
        args(0)
      }
      case _ => "local[1]"
    } 
    val employeeNameInput = args.length match {
      case x:Int if x > 1 => {
        args(1)
      }
      case _ => "testdata\\hadoopexam\\input\\module18joinGroupBySwap\\employeeName.csv"
    } 
    
    val employeeSalaryInput = args.length match {
      case x :Int if x >2 => {
        args(2)
      }
      case _ => "testdata\\hadoopexam\\input\\module18joinGroupBySwap\\employeeSalary.csv"
    } 
   
    val resultInput = args.length match {
      case x: Int if x >3 => {
        args(3)
      }
      case _ => "testdata\\hadoopexam\\output\\module18joinGroupBySwap\\result"
    } 
    
    val sc = new SparkContext(master, "module18JoinGroupBySwap", System.getenv("SPARK_HOME"))
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val employeeName = sc.textFile(employeeNameInput)
    
    val employeeNameRdd= employeeName.map(x => (x.split(",")(0), x.split(",")(1)))
    
    println("Employee name RDD is")
    employeeNameRdd.foreach(println)
    
    val employeeSalary = sc.textFile(employeeSalaryInput)
    
    val employeeSalaryRdd = employeeSalary.map(x => (x.split(",")(0) , x.split(",")(1)))

    val threeTuppleEmployeeSalaryRdd = employeeSalary.map(x => (x.split(",")(0), x.split(",")(1), x.split(",")(1)))
    
    println("Employee Salary RDD")
    employeeSalaryRdd.foreach(println)
    
    val joinedRdd = employeeNameRdd.join(employeeSalaryRdd)

    println("Joined EmployeeName and EmployeeSalary by employee id ")
    //Note: But how would spark know how to join 
    
    joinedRdd.foreach(println)
    
    println("Checking to see how swap function works for tuple(String,(String , String))")
    joinedRdd.map(x => x.swap).foreach(println)
    
    println("swap function does not works for tuple(String, String, String)")
    
    //Remove the keys from joinedRdd
    val removedKeysRdd = joinedRdd.values
    println("Only the values from Joined RDD are ")
    removedKeysRdd.foreach(println)
    
    //Swap the data so salary is first tuple
    val swappedRdd = removedKeysRdd.map(x => x.swap)
    println("Swapped Rdd is")
    swappedRdd.foreach(println)
    
    // Group the data using salary amount 
    val salaryGroupdata = swappedRdd.groupByKey().collect()
    println("Grouping the employeee names by salary ")
    salaryGroupdata.foreach(println)
    
    //Now create a Rdd values for each key
    
    val rddByKey = salaryGroupdata.map{case(x, y) => x->sc.makeRDD(y.toSeq)}
    println("Getting all the keys for each salary group data")
    rddByKey.map( x => println(x._1+"("+x._2.foreach(print)+")" ))

    //Save the data for each key 
    
    rddByKey.map{ case(k,rdd) => rdd.saveAsTextFile(resultInput+k)}
    
    
    
    
    
    
    
  }
  
  
}