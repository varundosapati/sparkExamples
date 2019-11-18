package org.spark.databricks.exmaples

import org.apache.spark.SparkContext
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.ContentExchange

object BasicMapPartitions {
  /*
   *TODO: Need to work to call the httpCall and check to see what happens
   */
  
  
  def main(args: Array[String]) :Unit = {
    
    val master = args.length match {
      
      case x: Int if x> 0 => args(0)
      case _ => "local"
    }
    
    val sc = new SparkContext(master, "BasicMapPartitions", System.getenv("SPARK_HOME"))
    
    val inputRdd = sc.parallelize(List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"))
    
    val result = inputRdd.mapPartitions{ 
          signs => {
            val client = new HttpClient()
            client.start()
            
            signs.map { sign => 
              val exchange = new ContentExchange(true)
              exchange.setURL(s"http://qrzcq.com/call/${sign}")
              client.send(exchange)
              exchange
            }.map{
              exchange =>
                exchange.waitForDone()
                exchange.getResponseContent
            }
          }
      }
    
    println("Result from records "+result.collect().mkString(","))
    
  }
  
}