Explanation of Sql Encoders

1) Usage of Implicit Object 
	import spark.implicit._
	By having above statement we are going to show the what kind of serialization and de serialization concepts needs to be used 

2) 	Explanation about Encoder 
	Encoder (Special ser-De for sparkSQL)
	
	In sparkSQL there are already existing encoder available for primitive data types like Int, String, Float, Double and some collections 
	Encoder are define with StructType and StrickField for each element of a case class(Javabean)
	
3) Why encoders are fast 
	Instead of using java ser-de or kyro serialization sparkSql uses it own serialization and de serialization. 
	The main performance comes because instead of de serializing the data for sorting, filter it does that in binary format which is sparkSQl gains lot of performance from different ser-de.
	
4) Creating custom Encoders 
	If you are having certain dataType which does not provides by SparkSql Encoder 
	We can use org.apache.spark.sql.Encoder for creating custom encoders 

5) Expression Encoders 
	Uses by the catalyst optimizer for specifying the expression tress //TODO - need to research on this		 
	 
