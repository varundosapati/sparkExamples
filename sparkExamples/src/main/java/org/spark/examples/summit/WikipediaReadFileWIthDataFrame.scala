package org.spark.examples.summit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._





object WikipediaReadFileWIthDataFrame {
  
  
  def main(args : Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("WikipediaReadWithDataFrame").getOrCreate()

    import spark.implicits._
    
    val rdd = spark.sparkContext.textFile("testData/wikipediapageCount.gs")
    
    
    val parsedRdd  = rdd.flatMap( 
                            line => line.split("""\s+""")  match {
                              case Array(project, page, numRequests, _) => Some((project, page, numRequests))
                              case Array(_) => None
                                  }
                              )
    
    //Convert to RDD                          
    val df = parsedRdd.toDF("prject", "page", "numRequests")
    
    //filter page as en , group by page, sum the numRequests and then agg
    
    df.filter($"page"=== "en").groupBy($"page").agg(sum($"numRequests").as("count")).limit(100).show()
    
    
    
//    parsedRdd.foreach(println)
    
    

    //filter only english language 
    
//     var df = parsedRdd.toDF("project", "page", "numRequests")
     
     
  
  }
}