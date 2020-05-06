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

	example 
		case class Course(id: Int, name:String, fee:Int, venue:String, duration:Int)
		import org.apache.spark.sql.Enoders 
		//Create and encoder for case class
		val caseEncoder = Encoder.product[Course]
		//Catalyst optimizer uses the expression encoder for Serialization and de 	Serialization of object we need to import catalyst expression encoder 
		import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
		//To find out how catalyst optimizer uses the serialization and de serialization we can find out below 
		courseEncoders.asInstanceOf[ExpressionEncoder[Course]].serializer
		courseEncoders.asInstanceOf[ExpressionEncoder[Course]].deserializer
		//Now lets add a course using the encoders, catalyst optimizer uses encoder serialization when sending over network
		val serializeRow = courseEnoders.asInstanceOf[ExpressionEncoder[Course]].toRow(Course(1, "Hadoop", 5000, "New York", 5))
		//Now lets get back original object we need to use resolveAndBind 
		import org.apache.spark.sql.catalyst.dsl.expressions._
		val deserializeRow = courseEncoders.asInstanceOf[ExpressionEncoder[Course]]
                          .resolveAndBind(Seq(DslSymbol('id).int, 
                              DslSymbol('name).string , DslSymbol('fee).int, 
                              DslSymbol('venue).string, DslSymbol('duration).int)).fromRow(serializeRow)