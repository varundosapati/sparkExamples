Spark sql create the operation(spark code) in AST (Abstract Syntax Tree)

About Catalyst Optimizer 

1) What is catalyst Optimizer
	Is the main back bone for spark for optimize for advanced programming features 
		eg;scala pattern matching , quosquotes etc
	
	Catalyst library 
		
	It support two types of optimization 
		Rule Based Optimization 
			
		Cost Based Optimization 
	
		Catalyst optimizer is based on tree transformation framework 
		
		Analyze logical plan to resolve reference - Rule based 
		Logical plan optimization - Rule Based			
		Physical plan [It may generate multiple plan ] - Cost based
		Code generation : Rule based 
		
2) Concepts of Tree and Rules 
	a) Different Stages of Tree
		Each phase  uses different types of tree nodes 
		
		Hence catalyst library is a library of node 
		1)Expression nodes
		2)nodes of datatypes
		3)node of logical operators 
		4)node of physical operators 
		
	b) Rules 
			i) 
			ii) 
3)Various Phases of Catalyst Optimizer

	a) Analysis Phase
		In Spark sql you work on Dataframe API, SQL Query which is converted in Abstract Syntax tree
		When AST is created there can be some unresolved attributes as such valid column name or dataType of column 
		
		To resolve this it uses some thing like HE-catalog
			
		Unresolved Attributes are use catalog object and catalog rules for converting unresolved plan to resolved plan  
		 	  
	b) Logical Optimization
		This is rule based optimization in logical plan 
		It uses standard scala features
			Constant folding 
			Predicate push down 
			Physical operator 
			Project pruning
			Null propagation 
			Boolean expression simplification
	c) Physical Planing
		This is based on cost based optimizer 
		
		Takes logical plan and applies different forms of operators and create all forms for physical plan choose the least cost 
		
	d) Code Generator
		Generate byte code (class file) to run 
		In this step code is created and executed on  each node 		 
		Spark uses quosicode a scala feature to generate jave byte code which is more efficient form 
		
4)Scala Features Concepts 
	a) Constant Folding
		This is compilation techniques in scala 
		Helps to compute the expression once for all rows and not to repeat this for each row 	
	
	b) Predicate Pushdown				 
		Predicate mean when clause in spark sql 
		In dataframe it is filter 
		You pass this predicate to the direct data source when getting the data
			eg:csv, json, rdbms tables, hive tables
	c) Project prunning 
		Only works on the columns require for query operation, so read only rquired columns 
		
	d) Physical operators
		Dataframe is finally converted into RDD 
		Hence all the actual operators of RDD needs to be derived
	