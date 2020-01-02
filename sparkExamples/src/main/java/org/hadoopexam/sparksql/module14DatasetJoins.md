 Spark Joins operations 
 
 Like RDD joins we have joins in dataset 
 
 Command in Linux for finding all spark-shell runnning 
 
 ps -aef | gep -i spark 
 
 
 Command for killing a process in linux 
 
 kill -9 2396 2467
 
 
1) Broadcast Joins 
 	BroadCast is small dataset to each node  we would like to share in cluster 
 	
 	if size of one of the dataset is small (means) a parameter is used to configure the same 
 		spark.sql.autoBroadCastJoinThreshold
 		
 	In hadoop world broadcast is known as 
 		1) Map side join 
 		2) Replicated join 
 		
 		
2) Dataset joins 
 	Inner Join - Common elemnts in two dataset
 	Left join - Common elements plus all elements in left dataset 
 	Right Join - common elements plus all elements in right dataset
 	Full outer join - Common elemnts in each dataset plus adding elements of eacg dataset 
 	
	Also in joins we can use operations directly if we have any complex conditions  	

3) Dataset Joins and Hints 	

	If a programmer would like to provide an hint to catalyst optimzer when doing a join.