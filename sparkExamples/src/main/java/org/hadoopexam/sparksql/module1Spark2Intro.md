Introduction to Spark 2.0 (SparkSQL)
1) Whats New in Spark Sql 
	Dataset API (scala/java)
	Dataframe API (scala/python/java)
	

2) What are the main goals in Spark Sql 
	Data stored in hive, external files , from sprak (streaming, rdbms)
	performance improvement 
	If new data formats needed to be added then it should easy(eg json, csv, csv e.t.c)
	High performance with other library 
		-Graph
		-Streaming data 
		-machine learning  
	
3) Start Datasets/DataFrame API
	Dataset Dataframe API mean you can debug intermediate results using standard tools 
	As you create a Dataframe/Dataset its logical plan would be created eagerly

4) Data Pipelines 
	operations works as in pipeline format
	
5) Schema Inference 
	java/scala - uses the case class or java beans 
	python - dynamic language, Extrada smaple data and then derives it Datatype and column names  

6) Column format 
	Spark sql by default saves intermediate data or final data as columnar format which is parquet

7) Introduction to UDF 
	You can create inline UDF functions 
	You can simply register then if you want to use in the sql 
	Udf can also be written using - Java, python, scala