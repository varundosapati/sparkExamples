package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 * Usage - We are going Relational Group Dataset for usage of groupByKey and Functions agg
 * 
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module15RelationalGroupedDataset\
 * args(2) - testdata\hadoopexam\sparkSql\output\module15RelationalGroupedDataset\
 *
 */

object module15RelationalGroupedDataset {
   Logger.getLogger("org").setLevel(Level.ERROR)
  case class Training(ID:Int, Name:String, Fee : Int, Venue:String, Date: String, Duration:String)

  def main(args : Array[String]) : Unit = {
    
    if(args.length < 3) {
      println("USAGE INPUTLOC OUTPUTLOC")
      System.exit(1)
    }
    
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module15RelationalGroupedDataset")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    
    
    val df1 = sparkSession.read.format("csv").option("header", true).option("Inferschema", true).load(inputLoc+"Training.csv")

    val ds1 = df1.as[Training]
    
    ds1.createOrReplaceTempView("TRAINING")
    
    //Cache the table 
    sparkSession.sql("CACHE TABLE TRAINING")
    
    /*
     * Two different ways to check whether data is stored is
     * 
     * UI : http://ipaddrss:sparkportnumber/storage 
     * commandline : sparkSession.catalog
     */
    print("Is TRAINING DATA Cached"+sparkSession.catalog.isCached("TRAINING"))
    
    //Clear cache 
    
    sparkSession.sql("CLEAR CACHE")
    
    print("AFTER CLEARING CACHE checking Is TRAINING DATA Cached"+sparkSession.catalog.isCached("TRAINING"))
    
    /*
     * Working with MEMORY_ONLY and MEMORY_AND_DISK storage 
     */
    sparkSession.catalog.cacheTable("TRAINING", StorageLevel.MEMORY_ONLY)

    println("Selecting the records which are cached ")
    sparkSession.sql("select * from TRAINING")
    
    sparkSession.sql("CLEAR CACHE")
    
    sparkSession.catalog.cacheTable("TRAINING", StorageLevel.MEMORY_AND_DISK)

    println("Selecting the records which are cached MEMORY AND DISK")
    sparkSession.sql("select * from TRAINING")
    
    
    /*
     * Aggregation opperation which creates RelationalGroupedDataset
     */
    import org.apache.spark.sql.functions._
    
    println("Get sum of fee for each venue")
    sparkSession.sql("SELECT SUM(FEE) from TRAINING GROUP BY VENUE").show()
    
    
    println("Using dataset get sum of fee collected")
    ds1.agg(sum($"fee") as "TotalFee").show()
    
    println("Using dataset to get the sum of fee for each venue")
    ds1.groupBy($"venue").agg(sum($"fee") as "TotalFee").show()
      
    println("Using dataset to get the average of fee collected in each venue")
    ds1.groupBy('venue).agg(avg('fee) as "AverageFee").show()

       println("Using dataset to get the max  fee collected in each venue")
    ds1.groupBy("venue").agg(max("fee") as "Max Fee").show()
    
    println("Count for each city count ")
    ds1.groupByKey(x => x.Venue).count().show()
    
    println("Now apply multiple aggregators")
    ds1.groupBy($"venue").agg(sum('fee)as "TotalFee", max('fee), min('fee), count('fee) , avg('fee)).show()
    
    /*
     * Lets look at more indepth agg and groupBy function using dataset 
     */
    
    println("select Total fee paid for each venue and course")
   val feeForVenueAndCourse = ds1.groupBy($"Name", $"venue").agg(sum('fee) as "TotalFee").select('Name,     'Venue, 'TotalFee)
    feeForVenueAndCourse.show()
    
    println("Select total amount collected for each venue")
    
    val feeForVenue=ds1.groupBy("Venue").agg(sum("Fee") as "TotalFee" ).select($"Venue" , lit("Total Price from this Venue") as "Name" , $"TotalFee")
    
//    val feeForVenue = ds1.groupBy("Venue").agg(sum($"Fee") as " TotalFee").select($"Venue", lit("Total Price collected from this venue") as "Name", $"TotalFee")
//    val feeForVenue = ds1.groupBy("venue").agg(sum($"fee") as " TotalFee").select($"venue", lit("Total Price collected from this veneue") as "Name",  "TotalFee")
    feeForVenue.show()
    
    println("Select total price collected for each course")
    val feeForCourse = ds1.groupBy("Name").agg(sum("Fee") as "TotalFee").select(lit("Total price from this course") as "Venue", $"Name" , $"TotalFee")
    feeForCourse.show()
    
    
    println("Now lets union the feeForVenueAndCourse, feeForVenue and feeForCourse ")
    val unionResult = feeForVenueAndCourse.union(feeForVenue).union(feeForCourse).sort($"Venue".asc_nulls_last)
    unionResult.show()
    
    println("Now saving the data to an parquet file")
//    unionResult.write.format("csv").save(outputLoc+"UnionResult_csv")
    
    
    /*
     * Let work on groupBy and agg using sql function in sparkSession 
     */
   
    
    ds1.createOrReplaceTempView("TRAINING")
    
    /*
     * Desired SQL Query
     * Grouping set is equivalent of Union of each group by operations 
     * which will provide total of each course per venue and grand total fee per venue
     */
   println("Selecting total Fee for each course per venue and also total fee per each venue ") 
   val totalOfEachCorusePerVenueAndGrandTotalForVenue =  sparkSession.sql("""
        SELECT Venue, COALESCE(Name, "Total price for this venue") as Name, SUM(Fee) as TotalFee 
        FROM TRAINING
        GROUP BY Venue, Name 
        GROUPING SETS ((Venue, Name), (Venue))
        ORDER BY Venue ASC NULLS LAST, Name ASC NULLS LAST
      """)
    
    totalOfEachCorusePerVenueAndGrandTotalForVenue.show()
    
    /*
     * grouping sets is equivalent of each group by operation 
     * which will provide total as well as grand total 
     * In below example we will get total fee per course per venue and per venue total fee and grand total of all venues
     */
   println("Selecting total Fee for each course per venue and also total fee per each venue and also total fee for all the venues")    
    val feeOfVenuePerCourseAndFeeOfVeneuAndTotal = sparkSession.sql("""
        SELECT Venue, COALESCE(Name, "Total Price for this Venue") as Name, SUM(Fee) as TotalFee
        From TRAINING
        GROUP BY Venue, Name
        GROUPING SETS ((Venue, Name), (Venue), ())
        ORDER BY Venue ASC NULLS LAST, NAME ASC NULLS LAST 
      """)

      feeOfVenuePerCourseAndFeeOfVeneuAndTotal.show()  
    
    
      feeOfVenuePerCourseAndFeeOfVeneuAndTotal.repartition(1).write.format("csv").save(outputLoc+"SQLGroupingFeePerVenueCoursePerVenue")
    
    
       /*
     * grouping sets is equivalent of each group by operation 
     * which will provide total as well as grand total 
     * In below example we will get total fee per course per venue and per venue total fee and grand total of all venues
     */
   println("Selecting total Fee for each course per venue and also total fee per each venue and total fee for course also total fee for all the venues")    
    val feeOfVenuePerCourseAndFeeOfVeneuAndFeeForCourseAndTotal = sparkSession.sql("""
        SELECT Venue, COALESCE(Name, "Total Price for this Venue") as Name, SUM(Fee) as TotalFee
        From TRAINING
        GROUP BY Venue, Name
        GROUPING SETS ((Venue, Name), (Venue), (Name), ())
        Order by Venue ASC NULLS LAST, NAME ASC NULLS LAST 
      """)

      feeOfVenuePerCourseAndFeeOfVeneuAndFeeForCourseAndTotal.show()  
    
    feeOfVenuePerCourseAndFeeOfVeneuAndFeeForCourseAndTotal.repartition(1).write.format("csv").save(outputLoc+"SQLGroupingFeePerVenueCoursePerVenuePerCourse")
      
      val df2 = sparkSession.read.format("csv").option("header", true).option("Inferschema", true).load(inputLoc+"Learners_stats.csv")
    
    
    
    
    
  }
  
  
  
}