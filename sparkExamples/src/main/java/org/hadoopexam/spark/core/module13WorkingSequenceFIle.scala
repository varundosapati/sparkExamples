package org.hadoopexam.spark.core

import org.apache.hadoop.io._
import org.apache.spark.SparkContext

/*
 * Working on sequence file 
 * 
 * 1) Creating text file with employee records on desktop as test\test.txt 
 	E01	Varun
 	E02	Test
 	  
 * 2) We need to load data in hive 
 * 		a) Create table in Hive with below command (Which creates a table to have data loaded from text file which tab separator for each field)
 * 			Create table employee (id String, name String) ROW FORMAT DELIMETED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE; 
 * 		b) Load data from text file into hive table
 * 			LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/test/test.txt' INTO TABLE employee
 * 		c) Now create sequence file to create a sequence from employee table 
 * 			Create table employee-seq (id String, name String) STORED as SEQUENCEFILE LOCATION '/test/test.seq' 
 * 		d) Now load data from employee table to sequence table 
 * 			INSERT INTO TABLE employee_seq select id, name FORM employee;
 * 	
 * 3) Now make sure sequence file is create in /test/test.seq in hdfs env
 * 
 * 4) Sequence file created is in below format 
 * 		a) key is org.apache.hadoop.io.BytesWritable
 * 		b) We have not a set a key value hence is set to NULL for all sequenceFile records
 * 		c) value is org.apache.hadoop.io.Text
 * 		d) Value contains all columns separated by '\01' by default 
 * 
 **/


/*
 * TODO - Need to make this where it can be run as work flow like in scoop
 */

 
object module13WorkingSequenceFIle {

  
  def main(args : Array[String]) : Unit = {

    val master = args.length  match {
      case x  if x > 0 => args(0)
      case _ => "local[1]"
    }
    
    val sc = new SparkContext(master, "module13WorkingSequenceFIle", System.getenv("SPARK_HOME"))
    
    val file=sc.sequenceFile[BytesWritable,String]("hdfs://quickstart.cloudera:8020/test/test_seq")
    //Load the file
val values = file.map(x => x._2.split('\01')).map(x => (x(0), x(1))) 

//Generate key and values from Sequnce File
values.collect 
//Just check the values
values.sortByKey(false).collect 
//Sort the content
val data = sc.sequenceFile("hdfs://quickstart.cloudera:8020/test/test_seq", classOf[BytesWritable],
classOf[Text]).map{case (x, y) =>(x.toString, y.toString)} 
    //Other way to load same file
println(data.collect().toList) 

//print the content
  }
  
  
}
