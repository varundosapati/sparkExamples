package org.hadoopexam.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * Usage - We are going to create DataFrame and DataSet from SparkContext
 * Note we have to create sparkSession to get implicit object of convert Rdd to DataFrame(DF)/DataSet(DS)
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\output\module5DatasetAndDataFrame\
 *
 */

object module5DatasetAndDataFrame {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Define case class with Course Details
  case class Course(id: Int, name: String, fee: Int, venue: String, duration: Int)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("USAGE MASTER OUTPUTLOC")
      System.exit(1)
    }

    val master = args(0)
    val outputFile = args(1)

    val sparkContext = new SparkContext(master, "module5DatasetAndDataFrame", System.getenv("SPARK_HOME"))

    //Create an Rdd with 5 Course
    val inputRdd = sparkContext.parallelize(Seq(
      Course(1, "Hadoop", 6000, "Mumbai", 5),
      Course(2, "Spark", 5000, "Pune", 4), Course(3, "Python", 5000, "Hyderbad", 5), Course(4, "Scala", 4000, "Kolkata", 3), Course(5, "Hbase", 7000, "Banglore", 3)))

      inputRdd.foreach(println)
      
    val sparkSession = SparkSession.builder().master(master).appName("module5DatasetAndDataFrame").getOrCreate()

        import sparkSession.implicits._

      
    //Now Convert the above RDD into dataSet , As RDD is infer with schema is automatically converts that for dataset
    
    val inputDs = inputRdd.toDS()  
    
    println("Showing Data after for inputRdd is converted to inputDs")
    inputDs.show()
      
    val inputDf = inputDs.toDF()
    
    //Now lets select courses conducted in mumbai and having prices more than 5000
    //NOTE : using selet which give back the DataFrame with specified columns 
    
    val filteredCoursesDf = inputDs.where('fee > 5000).where('venue==="Mumbai").select('name, 'fee, 'duration)
    
    println("Displaying course records which having fee > 5000 and venue as Mumbai ")
    filteredCoursesDf.show()
    
    //Now instead of using the SparkSQL API  we can do the same thing in SQL
   inputDs.registerTempTable("courses")
   
   val filteredSqlDS = sparkSession.sql("SELECT name, fee, duration from courses where fee > 5000 and venue == 'Mumbai'")
      
   println("Now Displaying course records running a SQL statement with condition of having fee > 5000 and venue as Mumbai")
   
   filteredSqlDS.show()

  /*
   * Without adding this getting java.lang.IllegalArgumentException: Illegal pattern component: XXX 
   * 
   * .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").mode(SaveMode.Overwrite) 
   */
   
   
   filteredCoursesDf
     .coalesce(1) 
     /*
     * Returns a new Dataset that has exactly numPartitions partitions, when the fewer partitions are requested. If a larger number of partitions is requested, it will stay at the current number of partitions. Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.
     * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like (e.g. one node in the case of numPartitions = 1). To avoid this, you can call repartition. This will add a shuffle step, but means the current upstream partitions will be executed in parallel (per whatever the current partitioning is). 
     */
     .write
//   .format("com.databricks.spark.csv")
   .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").mode(SaveMode.Overwrite)
//   .text(outputFile) // text data is not supported for dataFrame
//   .csv(outputFile)
    .json(outputFile) 
   
    //Some more operations on dataframes 
    
    println("Display the data types of each column in inputDf"+inputDf.dtypes.mkString("|"))
    
    println("Display number of records in inputDf "+inputDf.count)
    
    println("Display the column names in inputDf "+inputDf.columns.mkString("|") )

    println("Dropping name colum from inputDf")
    inputDf.drop("name").show()

    println("Displaying the content in json format for inputDf")
    inputDf.toJSON.show()
    
    println("select the location where coruse fee are between 4000 to 7000 and location not in mumbai ")
    inputDf.filter(inputDf("fee").between(4000, 7000)).filter(inputDf("venue") !== "Mumbai").show()
    
    
    println("Select the locations where course fee are greater than 5000 in inputDf")
    inputDf.filter(inputDf("fee") > 5000).select("venue").show()
    inputDf.filter(inputDf("fee") > 5000).select(inputDf("venue").alias("Location")).show()
    
    println("Sort the venue in projection ")
    inputDf.sort("venue").show()
    
    println("Select fee greater than 4000 and sort venue ascending order and fee descing order using sort")
    inputDf.filter(inputDf("fee") > 4000).sort($"venue", $"fee".desc).show()
    
    println("Select fee greater than 4000 and default sort for venue  and fee  using sort")
    inputDf.filter(inputDf("fee") > 4000).sort($"venue", $"fee").show()
    
    println("Select fee greater than 4000 and order by venue and fee using order by ")
    inputDf.filter($"fee" > 4000).orderBy("venue", "fee").show()
    
    
    println("Group by the fee ")
    inputDf.groupBy("fee").count().show()
    
    println("Below functions are used to remove null values from the table ")
    inputDf.na.drop().show()
    //TODO create and example with groupByKey on dataframe and then use filter and sort operation 
    
    
  
  }

}