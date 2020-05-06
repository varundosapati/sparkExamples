package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType


/*
 * Usage - We are going indepth example of create DataFrame and DataSet 
 * Note we have to create sparkSession to get implicit object of convert Rdd to DataFrame(DF)/DataSet(DS)
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module10HandsonDatasetDataFrameDepth\
 * args(2) - testdata\hadoopexam\sparkSql\output\module10HandsonDatasetDataFrameDepth\
 *
 */

object module10HandsonDatasetDataFrameDepth {
   Logger.getLogger("org").setLevel(Level.ERROR)
   
  case class Course(id:BigInt, name: String, fee : BigInt, venue : String, duration : BigInt)  
  
  def main(args : Array[String] ) : Unit = {
    
    if(args.length < 3) {
      println("USAGE MASTER INPUTLOC OUTPUTLOC")
//      System.exit(1)
    }
    
    
    val master = args.length match {
      case x:Int if x>0 => args(0)
      case _ => "local[1]"
    } 
    val inputLoc = args.length match {
      case x: Int if x> 1 =>  args(1)
      case _ => "testdata\\hadoopexam\\sparkSql\\input\\module10HandsonDatasetDataFrameDepth\\"
    }
    val outoutLoc = args.length match {
      case x:Int if x >2 =>  args(2)
      case _ => "testdata\\hadoopexam\\sparkSql\\output\\module10HandsonDatasetDataFrameDepth\\"
    } 
    
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module10HandsonDatasetDataFrameDepth")
    
    
    val sparkContext = new SparkContext(sparkConf)
    
    val courseRdd = sparkContext.parallelize(Seq(
        Course(1, "Hadoop", 6000, "Mumbai", 5),
      Course(2, "Spark", 5000, "Pune", 4), Course(3, "Python", 5000, "Hyderbad", 5), Course(4, "Scala", 4000, "Kolkata", 3), Course(5, "Hbase", 7000, "Banglore", 3)))

     
    courseRdd.foreach(println) 
    
    
    val sparkSesion = SparkSession.builder().config(sparkConf).getOrCreate()
    
    import sparkSesion.implicits._

    //Convert rdd to ds using sparkSession 
    
    val courseDS = courseRdd.toDS()
    
    //Different variants 
    //Type 1 using sql lambda function 
    
    println("Using Dataset SQL lambda function")
    courseDS.filter(record => record.fee > 5000).show()
    
    //Type 2 column based sql expression 
    println("Using Dataset column based SQL expression")    
    courseDS.filter('fee > 5000).show()
    
    //Type 3 sql DSL 
    println("Using Dataset filter using sql")
    courseDS.filter("fee > 5000").show()
    
    /*
     * Now lets see different variants explain plan
     */
    
        //Type 1 Explain  plan using sql lambda function 
    
    println("Explain plan for  Dataset SQL lambda function")
    println( courseDS.filter(record => record.fee > 5000).explain(true))
   
    
    //Explain plan Type 2 column based sql expression 
    println("Explain plan Dataset column based SQL expression")    
    println(courseDS.filter('fee > 5000).explain(true))
    
    //Explain plan Type 3 sql DSL 
    println("Explain plan Dataset filter using sql")
    println(    courseDS.filter("fee > 5000").explain(true))

    /*
     * Different methods in Dataset 
     * Loading two files 
     */
    
    //jsonDataTwoFIleDS is DataSet of COurse else it will be a DataFrame
    val jsonDataTwoFilesDF = sparkSesion.read.format("json").load(inputLoc+"data_1.json", inputLoc+"data_2.json")
  
    val jsonDataTwoFilesDS = jsonDataTwoFilesDF.as[Course] 
    println("Json data file in sparkSession")
    jsonDataTwoFilesDS.show()
    
    //Getting each file loaded 
    println("File names loaded are ")
    println(jsonDataTwoFilesDS.inputFiles(0))
    println(jsonDataTwoFilesDS.inputFiles(1))
    
    //To check whether dataset is local or not .it means when you run the collect and take methods, it check the dataset is available today, Hence no needed to run the executor on worker node if return is true 
    
    println("Checking to see dataset is local ")
    println(jsonDataTwoFilesDS.isLocal)
    
    //Returns all the colums in dataset 
    println("All columns in dataset are")
    jsonDataTwoFilesDS.columns.foreach(println)
    
    //Representing the column names and datatypes 
    println("Display the column names and datatypes")
    jsonDataTwoFilesDS.dtypes.foreach(println)
    
    //Schema for dataset are 
    println("Display the schema")
    println(jsonDataTwoFilesDS.schema)
    
    //Creating global view and local view 
    jsonDataTwoFilesDS.createTempView("V_TEMPCOURSE")
    jsonDataTwoFilesDS.createGlobalTempView("V_GLOBALCOURSE")
    
    //Select data from the local views
    println("Selecting data from temp local view")
    sparkSesion.sql("select * from V_TEMPCOURSE").show
    
    //selecting data from global view
    println("Selecting data from global view ")
    sparkSesion.sql("select * from global_temp.V_GLOBALCOURSE").show()
    
    //Converting Dataset to Dataframe (Strong Typed to genericType) and you even can rename the columns
    println("Trying to convert Dataset to Dataframe by giving different column names")
    val jsonDataTwoFilesdf = jsonDataTwoFilesDS.toDF("NUMBEROFDAYS", "FIXEDFEE", "COURSEID", "COURSE_NAME","TRAINING_VENUE")
    jsonDataTwoFilesdf.printSchema()
    
    println("changing dataframe column name with different names")
    
    jsonDataTwoFilesDF.toDF("NUMBEROFDAYS", "FIXEDFEE", "COURSEID", "COURSE_NAME","TRAINING_VENUE").printSchema()
    
    //Again converting dataFrame to Dataset
    println("Again converting DataFrame to Dataset")
    jsonDataTwoFilesDF.toDF("duration", "fee", "id", "name", "venue").as[Course].show()
    
    
    //Find Datatype information in case of DataSet and DataFrame
    println("Displaying dataset types")
    
    jsonDataTwoFilesDS.map(data => data.getClass.getName).show(false)
    
    println("Displaying dataframe types")
    jsonDataTwoFilesDF.map(data => data.getClass.getName).show(false)
    
    
    
    /*
     * Example of working with DataFrame 
     */
    
    //Create DataFrame from CourseRdd
    println("Create a dataFrame using inputRdd")
    val courseDf = courseRdd.toDF()
    courseDf.show()
    
    //Create an DataFrame with 5 Courses 
//    sparkSesion.createDataFrame(Seq(Course(1, "Hadoop", 6000, "Mumbai", 5),Course(2, "Spark", 5000, "Pune", 4),Course(3, "Python", 4000, "Hyderabad", 3) ,Course(4, "Scala", 4000, "Kolkata", 3),Course(5, "HBase", 7000, "Banglore", 7))).show() 
 
    //Creating DataFrame from csv file
    println("Reading data using com.databrick.spark.csv format and using headerOption for reading data in Training.csv file")
    val trainigDF = sparkSesion.read.format("com.databricks.spark.csv").option("header", "true").load(inputLoc+"Training.csv") 
   
    trainigDF.show()
    
    trainigDF.printSchema
    
    /*
     * Converting RDD to Dataframe with multiple columns and then lets try to use as[String] for creating the Dataset
     */
    val courseDF = courseRdd.toDF("id", "name", "fee", "venue", "duration")
  
    val coursedataset = courseDF.select("id").as[String]
    
    coursedataset.show()
    
    /*
     * Example of creating a schema with StructType and StructField and accessing csv data 
     * ID,Name,Fee,Venue,Date,Duration
     */
    println("Reading Training.csv data by adding schema and provinding header option")
    val schema = new StructType().add("ID", IntegerType, false)
                   .add("Name", StringType, false)
                   
//     val trainDf = sparkSesion.read.format("csv").schema(schema).load(inputLoc+"Training.csv").toDF("ID", "Name", "Fee", "Venue", "Date", "Duration")
     
     val trainDf = sparkSesion.read.format("csv").schema(schema).load(inputLoc+"test.csv").toDF() 
     trainDf.show()
    
  }
  
  
}