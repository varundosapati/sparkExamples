package org.spark.databricks.exmaples

//import org.apache.spark
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

  /*
   * Instruction for installing casendra on windows 
   * 
   * 1) Have python2.7 installed on (c:/python27)
   * 2) Set the environment variable with PATH
   * 3) Download cassandra and using 7gip place in (D:/cassandara) and place environment variable (d:/cassandra/bin)
   * 4) open command prompt run cassandra on first window(cassandra)
   * 5) open another command prompt run cqlsh 
   * 		a) describe keyspaces
   * 		b) create keyspace javalife WITH REPLICATION ={'class': 'SimpleStrategy', 'replication_factor':1};
   * 		c) use JavaLife;
   * 		d) create table javalife.test(name text, description text); 
   * 		e) describe tables;
   * 
   * 
   * This example is moved to spark-2-11-examples
   */

    
object BasicQueryCassandra {
 
  def main(args: Array[String]) : Unit = {
    
     if(args.length < 1) {
      println("Input format [local] ")
      System.exit(1)
    }
    
    val sparkMaster = args(0)
    val cassandraHost = "127.0.0.1:9042"
    
    
    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
    
    val sc = new SparkContext(sparkMaster, "BasicQUeryCassandra", sparkConf)
    
    println("Without datastax")
    
    /*
     * Select the total table into RDD 
     * 
     * Assume you table test was created as CREATE TABLE javalife.test(key text primary key, value int)
     * 
     */
    
//    val data = sc.cassandraTable("javalife", "test")
//    
//    //print some basic stats 
//    
//    println("stats" +data.map(row => row.getInt("value")).stats() )
//    
//    val rdd = sc.parallelize(List("moremagic", 2))
//    
//    rdd.saveToCassandra("javalife", "test", SomeColumns("key", "value"))
    
    //save from case class 
    
//    val otherRdd = sc.parallelize(List(KeyValue("magic", 0)))
    
  }
  
  
}