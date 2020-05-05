Project Tungsten 

1) Purpose of Project Tungsten
Improvements for spark application 
	memory usage 
	CPU usage 		
It is used for modern hardware efficient for better performance. 

Below are the benefits of using project Tungsten 
 
 a) Binary Processing 
 	All the operations are done in binary format. In java data is deserialized for comparison where as in Spark(scala) data is converted in binary formated and handles the sorting and filtering process
 	
 	Managing memory explicity and eliminate overhead of jvm object model and garbage collection
 
 b) Cache Aware Computation
 	Algorithms statred exploiting memory hierachy 	
 
 c) Code Generation
 	Code generation happens once and applies on all the data.
 	
 d) Custom Memory management 
 	Instead of using java memory mamanegment tungsten using its own memory managment for high  performance 

2) Shuffling in spark 
	When ever shuffeling happens in spark below components are used 
		i)Serialization
		ii)hashing 
		Both are cpu based and impact performance 
	a) Memory management 
		Java uses general byte allocation like String like="exam" takes in 48bytes in natie java for 4byte UTF08
	b) Cache aware computation 
		leveraging L1, L2, L3 cpu caches, cache aware sort is implemented which 3x faster than previous sorting 
	c) Code generation 
		Spark at runtime generate byte code for evaluating this expression 			 	