package org.hadoopexam.spark.core

import org.apache.spark.SparkContext

object model11ReduceByKeyFoldByKeyCombinerByKeyGroupBYKey {
  
  /*
   * Explanation - Pair Rdd Transformation examples 
   * 
   * reduceByKey - 
   * def reduceByKey(func:(v, v) => v) :RDD[(k,v)]
   * def reduceByKey(func:(v, v) => v, numPartitions : Int): RDD[(k,v)]
   * def reduceByKey(partitioner : Partitioner, func:(v, v) => v ): RDD[(v, v)]
   * 
   * 
   * foldByKey
   * 
   * def foldByKey(func(v, v) => v) : RDD[(k,v)]
   * def foldByKey(func(v, v) => v, numPartitions :Int): Rdd[(k, v)]
   * def foldByKey(partitioner : Partitioner, func(v, v) => v) : RDD[(v, v)]
   * 
   * combineByKey
   * 
   * def combineByKey[C](createCombiner: V => C, mergeValue:(C, V) => C, mergeCombiners:(C, C) => C ):RDD[(K,C)]
   * def combineByKey[C](createCombiner: V => C, mergeValue:(C, V) => C, mergeCombiners:(C, C) => C, numPartitions : Int ):RDD[(K,C)]
   * def combineByKey[C](createCombiner: V => C, mergeValue:(C, V) => C, mergeCombiners:(C, C) => C, partitioners: Partitioners, mapSideCombine : Boolean = true, serialiserClass : String = null )
   * 
   * 
   * groupByKey - Better not to use groupByKey on huge dataset because effects on performance, better to use on reduceByKey
   * 
   * def groupByKey() :RDD[k, Iterable[V]]
   * def groupByKey(numPartitions:Int) : RDD[K, Iterable[V]]
   * def groupByKey(partitioner: Partitioner): RDD[k, Iterable[V]]
   */
  
  
  
  
  
  
  def main(args: Array[String]):Unit ={
    
    val master = args.length match {
      case x  if x > 0 => args(0)
      case _ => "local[1]"
    }
    
    /*
     * Example of reduceByKey
     */
    val sc = new SparkContext(master, "MODEL11REDUCEBYKEYFOLDBYKEYCOMBINEBYKEY" , System.getenv("SPARK_HOME"))
    
        val input = sc.parallelize(List("dog", "cat", "owl", "ant", "gnu", "pig"), 2)
    
        val wordsInput = sc.parallelize(List("one", "two", "two", "three", "three", "three"), 1)

        val inputPairRdd = input.map(x => (x.length(), x))
    
        val wordsPairRdd = wordsInput.map(x => (x, 1))
        
        val inputReduceResult = inputPairRdd.reduceByKey(_+_)
    
        val wordsReduceResults = wordsPairRdd.reduceByKey(_+_)
        
        println("Result of pairRDD after reducing the content ")
        inputReduceResult.foreach(println)
    
        println("Result of words pairRDD after reducing the content ")
        wordsReduceResults.foreach(println)
        
    
        val input1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    
        val input1PairRdd = input1.map(x => (x.length(), x))
    
        val input1ResultRdd = input1PairRdd.reduceByKey(_+_)
    
        println("Result of pairRDD1 after reducing the values are")
        input1ResultRdd.foreach(println)

    /*
     * Example of fold
     *
     * TODO Getting Exception in thread "main" org.apache.spark.SparkException: Task not serializable
	at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:403)
     *
     */
    //     val inputFold = sc.parallelize(List("dog", "cat", "owl", "ant", "gnu", "pig"))
    //
    //    val inputFoldPairRdd = inputFold.map(x => (x.length(), x))
    //
    //    val inputFoldResultRdd = inputFoldPairRdd.foldByKey("")(_ + _)
    //    println("Result of pair RDD after doing a fold with initial empty value")
    //    inputFoldResultRdd.foreach(println)

    //    val input1FoldResultRdd = input1PairRdd.foldByKey("/")(_ + _)
    //    println("Result of pair RDD1 after doing a doing with / value is")
    //    input1FoldResultRdd.foreach(println)

    /*
     * Example of Combiner
     * Explanation -
     * Created a string list and interger list
     * Used zip to create a pair rdd as each number of lists match as mapc
     * Did a combineByKey tranformation on mapc and placed a initial element in List.
     * In the accumulator the Y value elemnt is added to X List
     * And finally we are combing the x list and y list together
     *
     *
     */

    val inputa = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val inputb = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val mapc = inputb.zip(inputa)
    val resultCombiner = mapc.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)

    println("After combing input a and inputb results are")
    resultCombiner.foreach(println)
  
    
    /*
     * Group By Key
     */
    
    val keyByInputa = inputa.keyBy(_.length())
    println("Grouping the keys of input with there length")
     keyByInputa.groupByKey().foreach(println)
     
      println("Grouping the keys of words input ")
     wordsPairRdd.groupByKey().foreach(println)
    
     val wordCountPairRdd = wordsPairRdd.groupByKey().map(x => (x._1, x._2.sum));
      println("Grouping the keys sum of words input ")
     wordCountPairRdd.groupByKey().foreach(println)
    
     
  }
}