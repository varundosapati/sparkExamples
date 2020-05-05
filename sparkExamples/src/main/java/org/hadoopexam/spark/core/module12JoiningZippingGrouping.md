We are going to discuss about 
	Topics
		1)reduceByKey VS GroupByKey performance issues 
		2)coGroup
		3)join(left, right, inner, outer)
		4)zip
		5)sort
		6)Cartesian
		7)takesample 
		
1) difference between reduceByKey and groupByKey and key performance issues
	reduceByKey
	 API 
	   * def reduceByKey(func:(v, v) => v) :RDD[(k,v)]
	   * def reduceByKey(func:(v, v) => v, numPartitions : Int): RDD[(k,v)]
	   * def reduceByKey(partitioner : Partitioner, func:(v, v) => v ): RDD[(v, v)]
	 
	 Explanation: reduceByKey combines the same key data locally before shuffeling data between nodes	
	
	groupByKey 
	 API
	   * def groupByKey() :RDD[k, Iterable[V]]
	   * def groupByKey(numPartitions:Int) : RDD[K, Iterable[V]]
	   * def groupByKey(partitioner: Partitioner): RDD[k, Iterable[V]]
	 Explanation: groupByKey send same key data to certain partition over the network, which moves all the data around so causes high network IO 
	 
2) cogroup 
	API 
		def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
		def cogroup[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))]
		def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))]
		def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
		def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1],Iterable[W2]))]
		def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1],Iterable[W2]))]
		def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
		def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]): RDD[(K, (Iterable[V], IterableW1], Iterable[W2]))]
		
	Explanation : It will combine/group all rdd from same key, provides to combine 3 key pair rdds. If a collection does not have matching key then an empty CompactBuffer is added
	Example : rdd1 : [(a, 1), (a, 2), (b, 3)], rdd2:[(a, 4), (a, 5), (a, 6)], rdd3:[(a, 6), (a, 7), (b, 8)]
		Result RDD : [(a, (1, 2, 4, 5, 6, 7)), (b, (3, 6, 8))]
		 
3) Join 
	Joins are similar to RDBMS 
	Left Outer Join 
		API :
		Left Outer Joins
		   * def leftOterjoin[W](other :RDD[(K, W)]):RDD[(K, (V, Option[W]))]
		   * def leftOterjoin[W](other: RDD[(K, W)], numPartitions:Int): RDD[(K, (V, Option[W]))]
		   * def leftOterjoin[W](other: RDD[(K, W)], partitioner:Partitioner):RDD[(K, (V, Option[W]))] 
		Explanation : All key by value of left pair RDD exists with joining data of right key by value RDD
	
	Right Outer Join 
		API :
		   * def rightOuterJoin[W](other : RDD[(K,W)]): RDD[(K, (Option[V], W))]
		   * def rightOuterJoin[W](other : RDD[(K,W)], numPartitions:Int): RDD[(K, (Option[V], W))]
		   * def rightOuterJoin[W](other : RDD[(K,W)], partitioner: Partitoiner): RDD[(K, (Option[V], W))]
		Explanation : All key by value of right pair RDD exists with joining data of left key by value RDD	 
		
	Full Outer Join
		API : 
		   * def fullOuterJoin[W](other: RDD[(K, W)]):RDD[(K,(Option[V],Option[W]))]
		   * def fullOuterJoin[W](other: RDD[(K, W)], numPartitions):RDD[(K, (Option[V],Option[W]))]
		   * def fullOuterJoin[W](other:RDD[(K, W)], partitoiner:Partitoiner):RDD[K, (Option[V], Option[W])]	
		  Explanation : All Keys from left and right RDD are exists with match keys are grouped together 
		  
4) Zip 
	API : 
		def zip[U:ClassTag](other:RDD[U]):RDD[(T,U)]
	Explanation : zip combines two RDD with matching elements and create a key by value Pair RDD

5) SortByKey
	Which sorts by default ascending order for keys or alphaNumeric sorting. Can be applied on the records with also have a capability of doing custom comparator 
	We can also create a custom comparator. Sort can be applied on pair RDDs			  
	
6) Cartesian
	Cartesian create a combination both rdd provided. For example you have 10 elements in first RDD and 10 elements of 2 RDD, it creates 10X10 elements as final output
	
7) takeSample		