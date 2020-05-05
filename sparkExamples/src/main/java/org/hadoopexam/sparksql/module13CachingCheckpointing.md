DataFrame DataSet Caching Checkpointing 

In this section we will see the different mechanisim of how Caching and Checkpointing works also the performance of each


1) DataSet Caching 
	If you want Dataset further you can cache it
	Options are MEMORY_ONLY, MEMORY_DISK(default)
	If MEMORY_ONLY option is set and not enough memory exist the application may fail
	
	dataset.persist()
	
2) DataSet unPersist
	If you no longer want the cached content then you can unpersist it 
	
	dataset.unpersist()
	
3) DataSet Eager Checkpointing 

	While you are working with dataset and want to freeze(persist data to disk) you can use checkpointing.
	
	By doing a checkpointing so far created lineage graph is truncated. If more transformations are used then new lineage graph is created   

	Checkpointing need to be used in iteration algorithm for better performance 
	
	Also with branching you can use checkpointing for better performance 
	
	Benefits : 
		Logical Plan will be truncated 
		Helpful in case of iterative algorithm and branching 
		save data to a file like hdfs 

	There are two ways of checkpointing 
		Eager Checkpointing 
		Non Eager Checkpointing
	
	Disadvantages :
		If you a have huge data then for saving the data it take some time
		 	
4) DataSet non eager Checkpointing 
	In this case lienage will not be cut 
	Even after creating checkpointing it will still use lineage 
	
5) Local Checkpointing
	Checkpointing does contents of an executor locally. So the data of processing in that executor is saved locally
	In respective performance it is not reliable as if machine gets crashed

6) Caching vs Checkpointing 

	dataset.persist(DISK_ONLY)
	dataset.checkpointing(eager)
	
	Persist(caching) will serialize data and keep the data either in cache(memory) or disk, If dataFrame is lost it remebers lineage and gets recreated
	
	Eager checkpointing will not store the linage and dataframe will be persisted in disk 
	
	
5) DataSet lineage truncation 

6) Dataset Performance Improvements 
	If you have n number of multiple transformation and actions in the middle.
	So for better performance you have 2 options 
		Using caching you can cache the data in each action and so that you will get little performance if dataFrame gets lost 
		
		Using checkpointing data is actually saved to disk. So truncation of lineage graph happens and rest of tranformation uses the saved data to apply remaining tranformations.
		
		So checkpointing provides more reliable performance. Only disadvantage is it takes time for saving data.
		