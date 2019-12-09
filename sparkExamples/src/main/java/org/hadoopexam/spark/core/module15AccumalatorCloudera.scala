package org.hadoopexam.spark.core

import org.apache.spark.SparkContext

object module15AccumalatorCloudera {

  /*
   * Example of accumulators which are created in cloudera frame work which was backed in running in haddop
   * 
   * For example 1-5 we have created 4 files in /user/cloudera/shared_variable named as file1.txt, file2.txt, file3.txt, file4.txt
   * 
   * file1.txt contains string of https://en.wikipedia.org/wiki/Apache_Hadoop
   * file2.txt contains string of https://en.wikipedia.org/wiki/Sqoop
   * file3.txt contains String of https://en.wikipedia.org/wiki/Apache_Spark
   * file4.txt containe String of https://en.wikipedia.org/wiki/Apache_Hive
   * 
   * Example 1 - 
   * 	Created an accoumulator named acc with inital value 0
   * 	pointed the shared_variable direct to allWords and did a flatMap for each for with regular expression \\W=
   *  looped through allWords variable and incremented count by 1 using += function
   *  did acc.value to get total words count in that directory
   *  
   *  
   * Example 2
   * 	Created an accoumulator named accFile1 with inital value 0
   * 	pointed the shared_variable/file1.txt direct to wordsFile1 and did a flatMap for each for with regular expression \\W=
   *  looped through wordsFile1 variable and incremented accFile1 count by 1 using += function
   *  did accFile1.value to get total words count of file1.txt
   * 
   * Example 3
   * 	Created an accoumulator named accFile2 with inital value 0
   * 	pointed the shared_variable/file2.txt direct to wordsFile2 and did a flatMap for each for with regular expression \\W=
   *  looped through wordsFile2 variable and incremented accFile1 count by 1 using += function
   *  did accFile2.value to get total words count of file2.txt
   *  
   * 
   * Example 4
   * 	Created an accoumulator named accFile3 with inital value 0
   * 	pointed the shared_variable/file3.txt direct to wordsFile3 and did a flatMap for each for with regular expression \\W=
   *  looped through wordsFile1 variable and incremented accFile3 count by 1 using += function
   *  did accFile3.value to get total words count of file3.txt
   * 
   * Example 5
   * 	Created an accoumulator named accFile4 with inital value 0
   * 	pointed the shared_variable direct/file4.txt to wordsFile4 and did a flatMap for each for with regular expression \\W=
   *  looped through wordsFile4 variable and incremented accFile1 count by 1 using += function
   *  did accFile4.value to get total words count of file4.txt
   * 
   * Example 6
   * 	Created an accoumulator named accCharCount with inital value 0
   * 	pointed the shared_variable direct to allCharWords and did a flatMap for each for with regular expression \\W=
   *  looped through allCharWords variable and incremented accFile1 count by  each word length using += function
   *  did accCharCount.value to get total number character words count of directory in shared-variable
   * 
   * Example 7
   * For example 6 created Employee.txt file in shared_variable folder with below text 
   	E01,John,36
    E02,Rakesh,27
    E03,Amit,45
    E04,Venkat,34
    E05,Chirag,29  
   *
   * In this example we created empCount accumulator with inital value 0
   * pointed the  shared_variable/Employee.txt direct to empRecords and did a map by splitting by ','
   * to get the total age of all employees looped through emprecords and added empCount with x(2).trim().toInt
   * did empCount.value to get the total age of all employees 
   * 
   * Example 8
   * For example 7 create Employee_bad.txt file in shard_variable folder with below text
	  E01,John,36
    E02,Rakesh,27
    E03,Amit,45
    E04,Venkat,M
    E05,Chirag,29
    E06,Jyoti,Femal
    E07,Chilu,? 
   *
   * In this example we created empBadCount accumulator with inital value 0
   * pointed the  shared_variable/EmployeeBad.txt direct to empBadRecords and did a map by splitting by ','
   * to get the total bad records  looped through empBadRecords and did a try catch block to find if there are any NumberFormatException
   * did empBadCount.value to get total number of bad records  
   */
  
  
  def main(args : Array[String]) : Unit = {

    val master = args.length match {
      case x:Int if x > 0 => {
        args(0)
      }
      case _ => "local[1]"
    }
 
    val sc = new SparkContext(master, "module15AccumulatorCloudera", System.getenv("SPARK_HOME"))
    
    //Example 1
    val ac = sc.accumulator(0)
    val allWords = sc.textFile("shared_variable").flatMap(_.split("\\W+"))
    allWords.foreach(w => ac += 1)
    println("TotalWords are "+ac.value)
    
    //Example 2
    val accFile1 = sc.accumulator(0)
    val wordsFile1 = sc.textFile("shared_variable/file1.txt").flatMap(_.split("\\W+"))
    wordsFile1.foreach(w => accFile1 += 1)
    println("TotalWords in file1 are "+accFile1.value)    

    //Example 3
    val accFile2 = sc.accumulator(0)
    val wordsFile2 = sc.textFile("shared_variable/file2.txt").flatMap(_.split("\\W+"))
    wordsFile2.foreach(w => accFile2 += 1)
    println("TotalWords in file2 are "+accFile2.value)

    //Example 4
    val accFile3 = sc.accumulator(0)
    val wordsFile3 = sc.textFile("shared_variable/file3.txt").flatMap(_.split("\\W+"))
    wordsFile3.foreach(w => accFile3 += 1)
    println("TotalWords in file3 are " + accFile3.value)
    
    //Example 5
    val accFile4 = sc.accumulator(0)
    val wordsFile4 = sc.textFile("shared_variable/file4.txt").flatMap(_.split("\\W+"))
    wordsFile4.foreach(w => accFile4 += 1)
    println("TotalWords in file4 are " + accFile4.value)

    //Example 6
    val acCharCount = sc.accumulator(0)
    val allCharWords = sc.textFile("shared_variable").flatMap(_.split("\\W+"))
    allCharWords.foreach(w => acCharCount += w.length)
    println("Total Character in Words in directory shared_variable are " +   acCharCount.value)

    //Example 7

    val empCount = sc.accumulator(0)
    val emp = sc.textFile("shared_variable/Employee.txt").map(v => v.split(","))
    emp.foreach(e => empCount += e(2).toInt)
    println("Total age of all employee are "+empCount.value)

    //Example 8
    val badrecords = sc.accumulator(0)
    val empAllRecords = sc.textFile("shared_variable/Employee.txt").map(v => v.split(","))
    empAllRecords.foreach(v => {
      try { v(2).toInt } catch {
        case e: NumberFormatException =>
          badrecords += 1
      }
    })
    println("Total Number of bad records are "+badrecords.value)
    
  }
  
}
