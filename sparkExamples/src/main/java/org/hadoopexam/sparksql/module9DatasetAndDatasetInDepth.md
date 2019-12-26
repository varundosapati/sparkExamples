Dataset 
	Dataset is logical plan for sparkSession 
	Logical plan describes all computation
	Dataset uses the Catalyst Optimizer and Project Tungston to optimize query performance 


1) Dataset variant operations
	Dataset can be created using below ways 
	a) Storages like files, hive tables, rdbms tables etc.
	b) Column based sql expression 
	c) lambda functions  

	So Dataset operation can be applied on below variants 
	i) Scala functions 
		dataset.filter(fee => fee > 5000).count
	
	ii)Coulmn based sql expression
		dataset.filter('fee > 5000).count
		
	iii) Sql Query 
		sql("select * from dataset where fee > 5000").count	 	

2) Dataset Compile time check 
	Dataset API is type safe. Dataset can provide syntax and analysis checks at compile time. 
	A Dataset object is formal repsentation of DataFram[ROW](Row object with proper column names and types)
	Dataset provice access of fields and does type casting automatically by compiler 
	
	
	
	DataFrames API are index based or column based (rows). It is not possible for compile time check on Dataframe, sql and RDD 
	In DataFrames Indices are used to access respective fields in dataframe and cast it to type.
	You can apply schema on DataFrame 
	Once you apply schema on DataFrame then they are same as Dataset 
	We use sqlContext to create DataFrame
	
	
	Below are the operation variants you can apply on DataFrame
		i) sql
		
		ii) Query DSL (it can check syntax at compile time)
		
	For DataFrame if schema is not specided it used generic column names 
	DataFrame["_c0", "_c1", "_c2"...]
	
3) Dataset transient values

	transient values are not required for serialize as those data is not being peristed  
	
	Dataset needs
		sparkSession  (@Transient)
		Query Execution plan (@Transient)
		encoder(ser-de)
		
	You can query dataset and persist the queried data
	
	
		
4) Converting DataFrames to Dataset 
	
	Ways for creating DataFrames to Dataset using caseClass , schemaType(StructType)
	
	We can convert DataFrame to Dataset using Case class 
	case class means infer the schema and type using reflection 
		df.as[T] = ds (case class can have only 22 fields)
	
	When number of columns are greater than 22 then we use programatic way to create schema(see below for more explanantion)	
	
	
5) Dataset using case class

	Below is an example of creating dataset of a case classDS
	case classDS(id: Int, name:String)
	
	val ds = dataFrame.as[classDS]

	Dataset can infer schema from Json and csv data directly as below 
	
	val ds: [classDS] = sparkSession.read.json(path)
	val ds: [classDS] = sparkSession.read.csv(path)
	
6) Dataset using programmatic schema
	
	Below are the reasons we usally use programmatic schema
	
	1)When your schema creation is dynamically and not known in advanced 
	2)If number fields are more than 22
	
	For programmatic schema creation we use StructType, (sequence of) StructFields 
	
	 val schemaType = StructType(
          StructField("id", LongType, nullable = false)  ::  
          StructField("name", StringType, nullable = true) ::
          StructField("fee", DoubleType, nullable = false) ::
          StructField("venue", StringType, nullable = false) ::
          StructField("duration", LongType, nullable = false) ::
          Nil
    )
	
	val df = sparkSession.read.format("csv").schema(schemaType)
						.load(path)
						.toDF("id", "name", "fee", "venue", "duration")
	
	For above examples column names and StructField names should match else we will get and exception							
	
7) Local Dataset 
	A Dataset considered local collection using below 
	
		sparkSession.emptyDataset
				
		sparkSession.createDataset
	In this case queries on Dataset can be optimized to run locally i.e with using spark encoders				