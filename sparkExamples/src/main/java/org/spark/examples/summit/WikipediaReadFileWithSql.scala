package org.spark.examples.summit

import org.apache.spark.sql.SparkSession


object WikipediaReadFileWithSql {
  
  def main(obj: Array[String]):Unit = {
    
    val spark = SparkSession.builder().master("local[0]").appName("WikipediaReadFileWithSQL").getOrCreate()
    
    import spark.implicits._
    
    val rdd =spark.read.textFile("testData/wikipediapageCount.gs")
    
    
    val parsedRdd = rdd.flatMap(line => line.split("""\s+""") match {
        case Array(project, page, numRequests, _) => Some((project, page, numRequests))
        case Array(_) => None
    })
         
    
    val df = parsedRdd.toDF("project", "page", "numRequests")
    
    df.createTempView("edits")
    
    val results = spark.sql("""select page, sum(numRequests) as requestCount from edits where project = 'en' GROUP BY page LIMIT 100""")
    
    results.show()
    
    
  }
  
  
}