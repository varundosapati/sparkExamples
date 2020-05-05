Topics 
	a)Spark Sql
	b)Catalyst Optimizer
	c)DataFrame 
	d) RDD vs Dataframes
	e)Hive Context

a)Spark Sql 
	Spark SQl can process data from 
		HDFS
		Cassandra
		Hbase
		RDBMS
b) Catalyst Optimizer :
	Does lot of optimizatin in each stage of the spark job, Benefits of optimization 
	a) makes adding of new optimization techniques
	b) enable external developers to extend optimizer 
	
	Spark SQL uses catalyst optimizer in 4 phases 
	1) Analyze logical plan to resolve reference 
	2) Logical plan optimizer 
	3) Physical planning 
	4) code execution to compile the parts of the query to java byte code 
	

c) DataFrame :
	Spark SQL uses programming abstraction called DataFrames. Dataframes are distributed collected in named columns
	DataFrame can be viewed as RDD of row objects allowing developer to call procedural API such as tranformation like map, flatMap and action as count, reduce e.t.c 
d) RDD vs DataFrames 
	RDD - RDD is an collection of objects with no idea about underlying data format 
	DataFrames - Have schema associated with it  			
		DataFrame = RDD + Schema