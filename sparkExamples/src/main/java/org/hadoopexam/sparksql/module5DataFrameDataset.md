Explanation of Spark Sql DataFrame and DataSet 

DataFrame is abstract of how data is displayed in a row from an RDBMS standpoint

From Spark 1.6 
DataSet came into picture which define the specific type of Data

Dataframe data can be represented in spark as DataSet[ROW] and in java as DataSet<Row>



DataFrame - 
	Dataframe is distributed immutable data.Data organized in named column 
	Dataframe used the catalyst optimizer 
	RDD does not used catalyst optimizer it is developers responsibility for optimizing the operations
	
	Java,
	Scala(Spark),
	Python,
	R

DataSet -
	Dataset do exist form spark 1.6 
	From spark 2.0 Dataset and Dataframe are combined together, Similar to Dataframe they are distributed data 
	
	Dataset[ROW] === Dataframe 
	Dataset<Row> == Dataframe 
	
	Dataset is a strongly typed JVM objects, You can use case class for referring element types  
	
	Dataset is available in below languages 
	Java,
	Scala(Spark)	