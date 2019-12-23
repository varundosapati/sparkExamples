Hands on Encoders 

1) we are going to create a case class 

2) we are going to import the Encoders 
	import org.apache.spark.sql.Enoders 

3) We are going to Create an Encoder for Case class 

4) Checking whether Encoder holds schema information or not and this schema is used to encode the object as row 

5) Also using ExpressionEncoder which helps catalyst optimizer during tree creation process 

6) Need to import 
	org,apache.spark.sql.catalyst.encoder.ExpressionEncoders
	
7) We create an instace of ExpressionEncoder

8) We will apply serialization on ExpressionEncoder to see what information exists

9) Also we can deserialize to check back the information we have just serialized 

	