Shared Variabled 
	a) Broadcast Variable
	b) Accumulators
	
a) Broadcast Variable 
	As name suggest you can broadcast some data from driver program to all the worker nodes, You can broadcast the data using sparkContext with a reference name which can be used in driver program for operations
	 
	spark.Boradcast.broadcast[T]
	
	Note: Broadcast variables are sent from only driver to worker nodes, which are only read only by worker nodes but worker cannot update on worker node 
		  In a worker node it can change the broadcast variable locally if the thery are not primitive datatypes and immutable objects 
b) Accumulators 		
	This are used to shared variables from worker to driver. worker node can update but cannot use them. Where as driver node cannot update the accumulators value
	
	Note: Accumulators can provide reliability  to make sure the the task sent to worker node is only executed once
	
	True Reliability : 
		Spark gaurntees that each task update to accumulator will only be applied once in actions(restored tasks will not update accumulators)
		Where as in case of transformations you must be aware each task update must be applied more than once if tasks are jobs are restored   