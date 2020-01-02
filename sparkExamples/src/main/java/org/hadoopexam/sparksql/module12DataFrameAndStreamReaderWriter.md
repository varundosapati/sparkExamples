About Reader and Writer in DataFrame, DataSet and Streams 

	Dataset contains list of Rows in JVM , Dataset has RowEncoder is attched to it by default if type is not defined. RowEncoder is attached for serialization
	
	Spark Sql Mode For reader and Writer 
		a) Batch Mode - Creating an application for reading from textFile, json, csv, rdbms, parquiet e.t.c 
		b) Streaming - Creating an application for reading from different streams such as socket, textFile e.t.c

	NOTE : For using dataFrameReader and dataFrameWriter we some times comes across errors becase of
		   missing implicit object like sparkSession.implicits._ and sparkSession.sparkContext.implicits._
		   
1) RowObject and schema 
	
	RowObject : 
		Is like a Array in java , Where RowObject contains ordered collection of differnet types 
		You can access fields via Index, column name , scala pattern matching 
		You can also option schema pattern for (org.apache.spark.sql.Row)
	
	RowSchema : 
		Row schema will always have schema attached to it if you are not specifying a schema 
		RowEncoder take care of assgining schema for a Row 
		
2) Row Encoder 
	Row Encoder is the default applies on DataFrame if no type is provided for serialization.
 	
 	
 	
 	
3) DataFrame Reader Interface 
 	Can read textFileUsing textFile method which return Dataset[String] instead of DataFrame 
 	
 	You get DataFramReader from sparkSession 
 	val dataFrameReader = sparkSession.read
 	Two ways of reading data is load, format specifi(json, csv, textFile, text e.t.c) which give DataSet
 	
 	Using dataFrame reader we can read data from csv, json, textFile, parquet, jdbc, orc
 	
 	Mainly encoder errors happens if we do not import implicit of sparkSession and sparkContext 
 	
 	By default spark loads data as parquet data, To change it you need to change value of 'spark.sql.sources.default'
 	
 	
 	
4) DataFrame Writer Interface 
	You get DataFrame writer from Dataset object to persist data into any format 
	
	DataSet.write.save("ouputFile") 
	
	Default save is parquet format 
	 	DataSet.write.format("csv").save("ouputFile") 
		DataSet.write.format("json").save("ouputFile") 

5) Schema Inference and Compression 
	You can specifiy the schema when you are reading data 
		sparkSession.read.schema(schemaType)
	InferSchema : csv and json formats can infer the schema from data itself 	

	Compression : Default compression of data is snappy.cmd other supported compression are 
		1)lzo 2)snappy 3)gzip 4)None
		dataFrame.write.option("compression", "none").save("myFile")
		 
6) Dataset Using programmatic scheam 
	We can provide the schema when reading the data from DataframReader
	

7) DataSet column 
	* represent all column in DataSet 
	
	There are different ways we can create a column which can be later added to dataset 
	
	Free Column Reference - Columns which are not associated to any dataset 
		a) org.apache.spark.sql.Column = 'name (This is a free column)
		b) $prefix - you can create column with $ prefix (You need to have sparkSession implicit objects)
			val nameCol : Column = $"name"(Another free column)
		c) Col and Column Function 
			val nameCol = col("name")
			val name = Column("city")
						
	Apply column to dataset or bound