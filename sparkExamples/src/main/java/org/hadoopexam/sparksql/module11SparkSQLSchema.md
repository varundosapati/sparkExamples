Apache Spark SQL schema creation and understanding 

Spark SQL schema gives or assign structure for data 

	In schema have 
		1) Name of the Column 
		2) Type of Column 
		3) NUllability 
	
	Spark SQL Dataset will have sceham 
		a) Inferring the schema = Runtime 
		b) Assign schema capability = Compile time
	
	
1) SparkSQL StructType and StructField 
	In Spark SQL schema is made of 
	StructType[<Collection of StructField>]
	StructType and StructFields are from the package of org.apache.spark.sql.types
	
	StructType contains different StructField which defines the schema 
	Base type of StructType is DataType (abstract class)
	For DataType some of the known sub classes are String , Int, Long e.t.c..

	Different ways of dataType representation 
	
	Catalog String : DataType stored in String format in external catalog
	
	JSON : Compact Json representation of datatype
	
	PrettyJson : Indented Json representation of datatype 
	
	SimpleString : Readable String representation of dataType 
	
	Sql : Sql presentation of datatype
	
	
	
	
2) Inferring Schema 
	Inferring schema is providing schema at runtime which provides catalyst optimizer the d


3) Printing schema in various ways 

4) Nested StructType
	StructType is representation of Row Object 
	A StructType can have inner StructType 
	
	StructFields : The fields which exists in StructType have 
		a) Name
		b) Type(Native)
		c) Nullability
	
5) Row object and accessing fields 
	Package of Row is org.apache.spark.sql.Row
	
	Row objects is a Serialized Object, It is a represent of a row in Dataset/Dataframe. You can access value from the index of column name.
	
	To create a new Row Object we can use Row.apply()
	Also Row object can be created by providing values Row("Hadoo", 5000, "Mumbai")	 
	


