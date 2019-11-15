package org.spark.examples.summit

import org.apache.spark.sql.SparkSession

object WikipediaReadFileWithRDD {
  
  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("WikipediaRead").getOrCreate()

    import spark.implicits._
    
    val rdd = spark.sparkContext.textFile("testData/wikipediapageCount.gs")
    
    
    val parsedRdd = rdd.flatMap{ 
                            line => line.split("""\s+""")  match {
                              case Array(project, page, numRequests, _) => Some((project, page, numRequests))
                              case Array(_) => None
                                  }
                    }
    
                              
//      parsedRdd.filter{ case(project, page numRequests) => project == "en" }.
//                map {case (_, page, numRequests) => (page, numRequests) }.
//                reduceByKey(_+_).
//                take(100).
//                foreach{ case(page, requeets) => println(s"$page: $requests")}
                
    
    
    //filter only english language 
    
//    parsedRdd.filter(case(project, page, numRequests) => page=="en")
                              
     parsedRdd.filter( content  => content._2 == "en").map(filteredContent => filteredContent._3).reduce(_+_).take(100).foreach(reducedContent => printf(s"$reducedContent._1: $reducedContent._2"))
  
  }
  
  
}