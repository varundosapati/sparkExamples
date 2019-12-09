package org.hadoopexam.spark.core

import org.apache.spark.SparkContext


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
      System.exit(1)
    }
    
    
    val master = args(0)
    val employeeNameInput = args(1)
    val employeeSalaryInput = args(2)
    val resultInput = args(3)
    
    val sc = new SparkContext(master, "module18JoinGroupBySwap", System.getenv("SPARK_HOME"))
    
    val employeeName = sc.textFile(employeeNameInput)
    
    val employeeNameRdd= employeeName.map(x => (x.split(",")(0), x.split(",")(1)))
    
    println("Employee name RDD is")
    employeeNameRdd.foreach(println)
    
    val employeeSalary = sc.textFile(employeeSalaryInput)
    
    val employeeSalaryRdd = employeeSalary.map(x => (x.split(",")(0) , x.split(",")(1)))

    println("Employee Salary RDD")
    employeeSalaryRdd.foreach(println)
    
    val joinedRdd = employeeNameRdd.join(employeeSalaryRdd)

    println("Joined EmployeeName and EmployeeSalary are")
    joinedRdd.foreach(println)
    
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
    rddByKey.foreach(println)

    //Save the data for each key 
    
    rddByKey.map{ case(k,rdd) => rdd.saveAsTextFile(resultInput+k)}
    
    
    
    
    
    
    
  }
  
  
}