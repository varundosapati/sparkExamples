package org.spark.databricks.exmaples

import org.apache.spark._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD

object BasicParseJsonWithJackson {
  /*
   * input parma - 
   * local
		testdata\databricks\Dark_Pool_Data_Feed_jackson.json
		testdata\databricks\output\Dark_Pool_Feed_Data_Jackson_Output.txt
   * 
   * 
   */
  
  case class DarkPoolData(tick:String, industry: String, sector: String, vol:String)
  
  case class DarkPoolDataList(darkPoolList: List[DarkPoolData])

  
  def main(args: Array[String]) : Unit = {
     if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      System.exit(1)
      }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val sc = new SparkContext(master, "BasicParseJsonWithJackson", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)
    
  
    /*
     * Parse it into specific case class. we use partitions because 
     * 
     * (1) ObjectMapper is not serialized so we either create a singleton object encapsulating objectMapper
     * 			on the driver and have to send data back to the driver to go through singleton object.
     * //Alternatively we can let each worker node create its own objectMapper but that's expensive in the Map
     * 
     * (2) To Solve for creating an objectMapper in each worker node without being expensive we create one per partitions 
     * 			with mapPartitions. Solves serialization and objectMapper creation perfomance hit 
     */
    
     val result = input.mapPartitions(records => {
       //mapper object created in each worker node 
       
       val mapper = new ObjectMapper with ScalaObjectMapper
       mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
       mapper.registerModule(DefaultScalaModule)
       
       /*
        * We use FlatMap to handle errors by returning an empty list None if we encounter an issue
        * and list with one element if everything is ok Some(_)
        * 
        */
       records.flatMap(record => {
         try{
           Some(mapper.readValue(record, classOf[DarkPoolDataList]))
         } catch {
           case e: Exception => None
         }
       })
       
     }, true)    
    
    
     result.collect().foreach(println)
     
     /*
      * We use flat map to get each of darkPoolDataList and use Map to covert darkPoolDataList to darkPoolData
      * Use filter to get the APPL records and saveToText file      
      */
     
     result.flatMap(records =>{ 
       records.darkPoolList 
       }).filter(record => record.tick.equals("AAPL")).mapPartitions({ records => 
       val mapper = new ObjectMapper with ScalaObjectMapper
       mapper.registerModule(DefaultScalaModule)
       records.map(mapper.writeValueAsString(_))       
     }, true).saveAsTextFile(outputFile)
     
     
     /*
      * Below code is used if there is only single DarkPoolData
      */
//     result.filter(record => record.tick.equals("AAL")).mapPartitions({ records => 
//       val mapper = new ObjectMapper with ScalaObjectMapper
//       mapper.registerModule(DefaultScalaModule)
//       records.map(mapper.writeValueAsString(_))       
//     }, true).saveAsTextFile(outputFile)
  }
}