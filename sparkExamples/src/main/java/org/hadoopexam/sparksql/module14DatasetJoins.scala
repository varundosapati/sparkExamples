package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 * Usage - We are going indepth example of Dataset and DataFrame Caching and Checkpointing 
 * 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module14DatasetJoins\
 * args(2) - testdata\hadoopexam\sparkSql\output\module14DatasetJoins\
 *
 */

object module14DatasetJoins {
   Logger.getLogger("org").setLevel(Level.ERROR)
  case class Course(id: Int, name:String , fee:Int, venue: String, duration:Int)  
  
  case class Trianing_Course(ID:Int, Name:String, Fee:Int, Venue:String, Duration:Int)
  
  case class Stats(ID : Int, Learners_Count: String , SubscribedFrom:String)
  
  
  def main(args: Array[String]) : Unit = {
    
    if(args.length < 3){
      println("USAGE master inputloc outputloc")
      System.exit(1)
    }
    
    
    val master = args(0)
    val inputloc = args(1)
    val outputloc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module14DatasetJoins")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    
    
    val df1 = sparkSession.read.format("csv").option("header", true).option("inferschema", true).load(inputloc+"Training.csv")
    
    println("First dataframe data ")
    df1.show()
    val df2 = sparkSession.sparkContext.parallelize(Seq( (1, "Hadoop", 6000, "Mumbai", 5), (2, "Spark", 5000, "Pune", 4), (3, "Python", 4000, "Hyderabad", 3))).toDF("ID","Name","Fee","City","Days") 
    
    println("Second dataframe data ")
    df2.show()
    
    //Inner join
    val joindf = df1.join(df2, "ID")
    println("Inner joing of df1 and df2")
    joindf.show()
    
    //Left Outer join 
    
    val leftJoin = df1.join(df2, Seq("ID"), "left").orderBy("ID")
    println("Left outer join content")
    leftJoin.show()
    
    
    //Right Outer join 
    
    val rightJoin = df1.join(df2, Seq("ID"), "right").orderBy("ID")
    println("Right outer join content")
    rightJoin.show()
    
    
    //full outer join 
   val fullJoin = df1.join(df2, Seq("ID"), "fullouter").orderBy("ID")
    println("full outer outer join content")
    fullJoin.show()
    
    /*
     * Join with operations 
     */
    
    val DS1 = sparkSession.sparkContext.parallelize(Seq(Course(1, "Hadoop", 6000, "Mumbai", 5),Course(2, "Spark", 5000, "Pune", 4),Course(3, "Python", 4000, "Hyderabad", 3) ,Course(4, "Scala", 4000, "Kolkata", 3),Course(5, "HBase", 7000, "Banglore", 7) ,Course(4, "Scala", 4000, "Kolkata", 3),Course(5, "HBase", 7000, "Banglore", 7) ,Course(11, "Scala", 4000, "Kolkata", 3),Course(12, "HBase", 7000, "Banglore", 7))).toDS() 
 
    val DS2 = sparkSession.sparkContext.parallelize(Seq(Course(1, "Hadoop", 6000, "Mumbai", 5),Course(2, "Spark", 5000, "Pune", 4),Course(3, "Python", 4000, "Hyderabad", 3))).toDS() 
    
    println("Example of joins using operations")
    println("Dataset 1 ")
    DS1.show()
    print("Dataset 2")
    DS2.show()
    
    //Now apply the joinWith operations 
    
    val innerDS = DS1.joinWith(DS2, DS1("ID") === DS2("ID"))
    println("Using join with operation for inner join of DS1 and DS2")
    innerDS.show()
    
    val rightDS = DS1.joinWith(DS2, DS1("ID") === DS2("ID"), "right")
    println("Right outer join of DS1 and DS2  with join with operation")
    rightDS.show()
    
    val leftDS = DS1.joinWith(DS2, DS1("ID") === DS2("ID"), "left")
    println("Left outer join of DS1 and DS2  with join with operation", "left")
    leftDS.show()

    val fullDS = DS1.joinWith(DS2, DS1("ID") === DS2("ID"), "fullouter")
    println("full outer join of DS1 and DS2  with join with operation", "left")
    fullDS.show()
    
    
    /*
     * Usage of Broadcast Hint
     */

    
    val trainingDf = sparkSession.read.format("csv").option("header", true).option("inferschema", true).load(inputloc+"Training.csv")
    
    val trainingDS = trainingDf.as[Trianing_Course]

    val statsDf = sparkSession.read.format("csv").option("header", true).option("inferschema", true).load(inputloc+"Learners_stats.csv")
  
    val statsDS = statsDf.as[Stats]
    
    //Do the joins by providing the broadcast hint '
    
    println("Providing statsDS as broadcast hint for catalyst optmizer ")
    
    trainingDS.join(statsDS.hint("broadcast"), "ID").show()
    
    println("Check to see the Logical plan when adding broadCast hint" + trainingDS.join(statsDS.hint("broadcast"), "ID").queryExecution.logical)
    
    println("Checking the parameter in spark conf for broadcast threshold size "+sparkSession.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt)
      
    
  }
  
  
}