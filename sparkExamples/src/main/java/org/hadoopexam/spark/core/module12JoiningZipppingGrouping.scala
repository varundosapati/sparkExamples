package org.hadoopexam.spark.core

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object module12JoiningZipppingGrouping {
  /*
   * CoGroup - It will combine/group all RDDS for the same key , A very powerful set of functions that allow grouping up to 3 key-value RDDs together using their RDDs 
   * 
   * def cogroup[W](other : RDD[(K, W)] ) : RDD [(K, (Iterable[V], Iterable[W]))]
   * def cogroup[W](other : RDD[(K, W)] , numPartitions : Int)[(K, W)] : RDD [(K, (Iterable[V], Iterable[W]))]
   * def cogroup[W](other : RDD[(K, W)], partitioner : Partitioner )[(K, W)] : RDD [(K, (Iterable[V], Iterable[W]))]
   * def cogroup[W1, W2](other1: RDD[(K, W1), other2:RDD[(K, W2)]]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
   * def cogroup[W1, W2](other1: RDD[(K, W1)], other2:RDD[(K, W2)], numPartitions:Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
   * def cogroup[W1, W2](other1: RDD[(K, W1)], other2:RDD[(K, W2)], partitioner:Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
   * def groupWith[W](other: RDD[(K, W)] ):RDD[(K, (Iterable[V], Iterable[W]))]
   * def groupWith[W1, W2](other1: RDD[(K,W1)], other2:RDD[(K, W2)]):RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
   * 
   * 
   * join - Joining datasets(RDDs) similar to we do in RDBMS/Datablase
   * 
   * Left Outer Joins
   * def leftOterjoin[W](other :RDD[(K, W)]):RDD[(K, (V, Option[W]))]
   * def leftOterjoin[W](other: RDD[(K, W)], numPartitions:Int): RDD[(K, (V, Option[W]))]
   * def leftOterjoin[W](other: RDD[(K, W)], partitioner:Partitioner):RDD[(K, (V, Option[W]))] 
   * 
   * 
   * Right Outer Joins 
   * def rightOuterJoin[W](other : RDD[(K,W)]): RDD[(K, (Option[V], W))]
   * def rightOuterJoin[W](other : RDD[(K,W)], numPartitions:Int): RDD[(K, (Option[V], W))]
   * def rightOuterJoin[W](other : RDD[(K,W)], partitioner: Partitoiner): RDD[(K, (Option[V], W))]
   * 
   * full Outer Joins
   * def fullOuterJoin[W](other: RDD[(K, W)]):RDD[(K,(Option[V],Option[W]))]
   * def fullOuterJoin[W](other: RDD[(K, W)], numPartitions):RDD[(K, (Option[V],Option[W]))]
   * def fullOuterJoin[W](other:RDD[(K, W)], partitoiner:Partitoiner):RDD[K, (Option[V], Option[W])]
   * 
   * Cross Joins  Inner Joins   
   * 
   * 
   * zip - Join two RDDS by combining the ith of either partitions with each other 
   * def zip[U:ClassTag](other:RDD[U]):RDD[(T,U)]
   * 
   * 
   * Sort - can be applied on the records with also have a capability of doing custom comparator
   * 
   * def sortByKey(ascending : Boolean , numPartitions : Int=selfpartitions.size) :RDD[P]
   * 
   * 
   */

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]) :Unit = {
    
    val master = args.length match {
      case x:Int if x > 0 => args(0)
      case _ => "local[1]" 
    }
    
    
    val sc = new SparkContext(master, "module12JoiningZipppingGrouping", System.getenv("SPARK_HOME"))

    /*
     * Exmaple of Cogroups
     */
    println("Example of Cogroups")
    val input = sc.parallelize(List(1, 2, 3, 1, 3), 1)
    
    val inputa = input.map((_, "a"))
    
    val inputb = input.map((_, "b"))
    
    val resultCogroup = inputa.cogroup(inputb)
    
    println("Result of cogroup of inputa and inputb is ")
    resultCogroup.foreach(println)

    val inputd = input.map(x => (x+1, "d"))
    
    val twoResultCogroup =  inputb.cogroup(inputa, inputd)
    
    println("Result of cogroup of inputa, inputb and inputd is ")
    twoResultCogroup.foreach(println)
    
     
    val x = sc.parallelize(List((1, "apples"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2) 
    
    val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "ipad")), 2)
 
    val resultCoGroupXY = x.cogroup(y)
    println("Result of cogroup of x, y is")
    resultCoGroupXY.foreach(println)
    
    y.cogroup(x).foreach(println)
    
    /*
     * Example of Join
     */
    
    println("Example of Left Outer Join / Right Outer Join / Full Outer Join ")
    val inputAnimals = sc.parallelize(List("dog", "salmon", "fish", "salmon", "rat", "elephant"), 3)
    
    val pairRddAnimalByLength = inputAnimals.keyBy(_.length())
    
    println(" pairRDDAnimalByLength content is")
    pairRddAnimalByLength.foreach(println)
    
    val inputBAnimals = sc.parallelize(List("dog", "cat", "gnu" , "salmon", "rabbit", "turkey", "wolf","bear", "bee"), 3)
    
    val pairRddBAnumalByLength = inputBAnimals.keyBy(_.length())
    
    println(" pairRddBAnumalByLength content is")
    pairRddBAnumalByLength.foreach(println)
    
    println("Result of left outer join pairRddAnimalByLength and pairRddBAnumalByLength")
    pairRddBAnumalByLength.leftOuterJoin(pairRddAnimalByLength).foreach(println)
    
    /*
     * Example of right outer join
     */
        println("Result of right outer join pairRddAnimalByLength and pairRddBAnumalByLength")
    pairRddBAnumalByLength.rightOuterJoin(pairRddAnimalByLength).foreach(println)
      
    /*
     * Example of full outer join
     */
    
    println("Result of full outer join of pairRddAnimalByLength and pairRddBAnumalByLength")
    pairRddAnimalByLength.fullOuterJoin(pairRddBAnumalByLength).foreach(println)

    /*
     * Example of Zip 
     * Make sure the number of elements needs to be same (Else there will be an exeception when elements gets done with action)
     * Also need to have same number of partitions (Else there will be IllegalArgumentException which cannot able to zip unequal partitions)
     */
    println("Example of Zip ")
    val inputNum = sc.parallelize(1 to 100, 3)
    
    val inputNum1 = sc.parallelize(101 to 200, 3)
    
    val resultNum = inputNum1.zip(inputNum)
    
    println("Result of both inputNum and inputNum2 is ")
    resultNum.foreach(println)
    
    val inputNum2 = sc.parallelize(201 to 300, 3)
  
    val resultNum1 = inputNum.zip(inputNum1).zip(inputNum2)
    
    println("Result of both inputNum, inputNum1 and inputNum2 is ")
    resultNum1.foreach(println)
    
    println("Result of both inputNum, inputNum1 and inputNum2 printing in array ")
    
    resultNum1.map(x => (x._1._1, x._1._2, x._2)).foreach(println)
    
    
    /*
     * Sort example  
     * 
     * Exaplanination = InputAnimals hold list of animals , creating CountRecordInputAnimal adding index of each element
     * 
     * Using the sortByKey for sorting the keys(animal names) in ascending and descending order 
     */
    
    val countRecordInputAnimal = sc.parallelize(1 to inputAnimals.count().toInt, 3)

    val pairRddOfAnimalsWithCounter = inputAnimals.zip(countRecordInputAnimal)
    
    println("Records in Pair RDD of PairRddOfAnimalwithCounter are")
    pairRddOfAnimalsWithCounter.foreach(println)
    
    val sortedRecordOfPairRddOfAnimalsWithCounter = pairRddOfAnimalsWithCounter.sortByKey(true)
  
    println("Records in Pair RDD of PairRddOfAnimalwithCounter after sorting in ascending order are ")
    sortedRecordOfPairRddOfAnimalsWithCounter.foreach(println)
    
    println("Records in Pair RDD of PairRddOfAnimalwithCounter after sorting in descending order are ")
    pairRddOfAnimalsWithCounter.sortByKey(false).foreach(println)
 
  
    /*
     * Using cartesian for another example of sorting the records using inputNum which has 1to100 in 5 partitions 
     * 
     * Using cartesian in it create  pair rdd of (1, 1) ... (100,100)
     */
    
    val cartesianRecordsInputNum = inputNum.cartesian(inputNum)
    
    println("Records of cartesian elements of 1to100 and 1to100 ")
    
    cartesianRecordsInputNum.foreach(println)
    
    val pickSomeElements = sc.parallelize(cartesianRecordsInputNum.takeSample(true, 5, 13), 2)
    
  
    println("Records of 5 elements from cartesian elements which are sorted as below")
    
    pickSomeElements.sortByKey(false).foreach(println)
    
  
  }
  
  
}