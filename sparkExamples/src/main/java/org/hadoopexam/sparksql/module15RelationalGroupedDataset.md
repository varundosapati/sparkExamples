We go through indepth into aggregate and groupByKey

	dataset.groupByKey(_.dept).agg(typed.sum[caseClass](_.salary)).toDF("Dept", "salary")

RelationalGroupedDataset 
	This is a return type of agg and groupByKey. Even if it is a return type of other operator
		groupBy , rollUp, cube, pivot
		
KeyValueGroupedDataset
	It is used to apply aggregators on Dataset (Typed manners)
			
	