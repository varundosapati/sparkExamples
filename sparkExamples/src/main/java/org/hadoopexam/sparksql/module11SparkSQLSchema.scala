package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * Usage - We are going indepth example of Spark SQL schema 
 * 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module11SparkSQLSchema\
 * args(2) - testdata\hadoopexam\sparkSql\output\module11SparkSQLSchema\
 *
 */

object module11SparkSQLSchema {
 Logger.getLogger("org").setLevel(Level.ERROR)
 
   case class HEEmployee(ID: Int, Name: String, gender : String, Salary: Int, Department:String) 
   
  def main(args : Array[String]) : Unit = {
    
    if(args.length <3 ) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module11SparkSQLSchema")
    
    val sparkContext = new SparkContext(sparkConf)

//    val sqlContext = new SQLContext(sparkContext)
//    import sqlContext.implicits._
    
    //Adding structfields one after another
    
   val schmeaDef = new StructType().add("course_id", IntegerType, false)
                   .add("course_name", StringType, false)
                   .add("course_fee", IntegerType, false)
                   .add("venue", StringType, false)

   //Another way of creating schema 
                   
   val schemaDef1 = new StructType().add("course_id", "int").add("course_name", "string")
                       .add("course_fee", "int").add("venue", "string")
                   
                   
   // Another way of creatinhg schema                 
   val schemaOtherDef = StructType( StructField("id", LongType, nullable = false) ::
                                         StructField("name", StringType, nullable = false) ::
                                         StructField("fee", LongType, nullable = false) ::
                                         StructField("venue", StringType, nullable = false) :: 
                                         Nil)                
   println("Schema defination is "+schmeaDef)

//    case class Course(id: Int, name: String, fee:Int, venue: String)
   
   val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
     import sparkSession.implicits._

   //Another way creating schema using DSL 
   
   val schemaDsl = new StructType().add($"course_id".int).add($"course_name".string)
                   .add($"course_fee".int).add($"course_venue".string)
   println("printing schemaDsl in tree Structure")
   println("Schema defination in pretty way"+schemaDsl.printTreeString())                
                   
   println("printiong schema in prettyjson format "+schemaDsl.prettyJson)

   /*
    * Another way of creating schema is using case class if scheam is known type and have less than 22 fields 
    * 
    * NOTE  : HEEmployee needs to be outside of the main method else toDF will not work
    * 
    */
    
   
   val HEEmployeeDS = sparkContext.parallelize(Seq( HEEmployee(1, "Deva", "Male", 5000, "Sales"), HEEmployee(2, "Jugnu", "Female", 6000, "HR"), HEEmployee(3, "Kavita", "Female", 7500, "IT"), HEEmployee(4, "Vikram", "Male", 6500, "Marketing"), HEEmployee(5, "Shabana", "Female", 5500, "Finance"), HEEmployee(6, "Shantilal", "Male", 8000, "Sales"), HEEmployee(7, "Vinod", "Male", 7200, "HR"), HEEmployee(8, "Vimla", "Female", 6600, "IT"), HEEmployee(9, "Jasmin", "Female", 5400, "Marketing"), HEEmployee(10, "Lovely", "Female", 6300, "Finance"), HEEmployee(11, "Mohan", "Male", 5700, "Sales"), HEEmployee(12, "Purvish", "Male", 7000, "HR"), HEEmployee(13, "Jinat", "Female", 7100, "IT"), HEEmployee(14, "Eva", "Female", 6800,"Marketing"), HEEmployee(15, "Jitendra", "Male", 5000, "Finance") , HEEmployee(15, "Rajkumar", "Male", 4500, "Finance") , HEEmployee(15, "Satish", "Male", 4500, "Finance") , HEEmployee(15, "Himmat", "Male", 3500, "Finance"))).toDS()
  
   println("Printing the schema from dataset of HEEmplyee"+HEEmployeeDS.schema)
   
   println("The catalog format of the dataset is"+HEEmployeeDS.schema.catalogString)
   
   println("The sql format of dataset is "+HEEmployeeDS.schema.sql)
   
   println("The JSON format of dataset is "+HEEmployeeDS.schema.json)
   
   println("The prettyJson format of dataset is "+HEEmployeeDS.schema.prettyJson)
   
   
   /*
    * Working with ROW for creating dataFrames
    * 
    * TODO : Need to find how if a column is not satisfying schema so that it can be ignored   
    */
   
   val course_detail = StructType(StructField("name", StringType, false) :: StructField("fee", IntegerType, false) :: StructField("city", StringType, false) ::Nil)
   
   println("Structure of defined schema in prettyJson"+course_detail.prettyJson)
   
   println("Structure of defined schema in tree string format")
   course_detail.printTreeString()
   
   //Create each row 
   val row = Row("Hadoop" ,5000,"Mumbai" ,400001 ) 
   val row1 = Row("Spark" ,5000,"Pune" ,111045 ) 
   val row2 = Row("Cassandra" ,5000,"Banglore" ,530068 ) 
   val row3 = Row(null ,5000,"Banglore" ,530068 ) 
   
   
   println("Accessing first value of row of column 1 "+row.get(0)+" column 2 "+row.get(1)+" coulmn 3"+row.get(2)+" column 4"+row.get(3) )
   
   val courseDf = sparkSession.createDataFrame(sparkContext.parallelize(Seq(row, row1, row2)), course_detail)   
   
   println("Displaying the dataframe records after applying schema")
   courseDf.show()
//   sparkSession.creat
   
  }
  
}