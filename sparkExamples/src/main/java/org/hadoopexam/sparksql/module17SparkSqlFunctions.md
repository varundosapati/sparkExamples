Spark SQL UDF Functions 

	a) Standard or User defined function 
	b) Aggregate Functions
	

We create UDF function for custom requirements 
UDF functions are not optmized by the SQL parser 
	
Standard or User defined functions
	In standard or user defined function we send single row as input and single row as output 
	
Aggregate function
	Aggregate function takes multiple rows as input and returns single row as output 
	

Windows aggregate function 
	It operate on group of rows, but calculate single value for each row in group 
	
		
	 
Different ways of creating UDF functions are 

a) Inline UDF function 
	import org.apache.spark.sql.types._ //Importing for IntegerTYpes
	val inLineUdfCalculateTotalSalWithBonus = udf( (sal:Int) => {sal * 20/100}+sal, IntegerTypes)	
	
b) Explicity creating UDF function 
    val calculateTotalSal = (sal:Int ) => {sal * 20/100}+sal
    
    val udfCalculateTotalSalWithBonus = udf(calculateTotalSal)

If you want to use UDF function in sparkSql you have register them 
	sparkSession.udf.register("function name", function definition)
	
			
	
 