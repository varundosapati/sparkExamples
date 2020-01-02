Spark SQL Roll Up Pivot Cube Operators 

	Rollup, pivot, cube operators of spark SQL operators and also on DataSet API .
	
Rollup Operator: 
	a)1 step generator total and then group by 
	b)Helps in getting total and subtotal 	
	c)You can do group by two columns then you can get total for each group and as well as entire dataset 
	d)You can use union operator to get similar results 
	
	

RelationalGroupedDataset 
	Grouping of rows 
	It is untyped 
	
	When you have one of the following operators on Dataset you will get RelationalGroupedDataset object 
		a)groupBy
		b)rollup
		c)cube
		d)pivot[It has to be used with groupBy operator]
		df.groupBy("name").agg(max("fee"), sum("fee")...)
		
	Remember you can not print or collect RelationalGroupedDataset 
	calling count() on grouped dataset is transformation and not action 
	
	
	ROLLUP :
	Here you can see the sum of the salaries of all employees grouped by their department. However, we cannot see the grand total, which is the sum of the salaries of all the employees belonging to all the departments in the company. 
 
	An alternative way to look at it is to say that the GROUP BY clause did not retrieve the total sum of the salaries of all the employees in the company. You can use ROLLUP operator for the requirement. 
 
	ROLLUP operator is helpful in calculating sub-totals and grand totals for a set of columns passed to the GROUP BY ROLLUP clause. 
 
	Let’s see how the ROLLUP clause helps us calculate the total salaries of the employees grouped by their departments and the grand total of the salaries of all the employees in the company. To do this we will work through a simple example query. 
 
	In this code, we used the ROLLUP operator to calculate the grand total of the salaries of the employees from all the departments. However, for the grand total ROLLUP will return a NULL for department. To avoid this, we have used the Coalesce clause. This will replace NULL with the text All Departments and display the department name of each department in the Department column.  
 	
 	Finding Subtotals Using ROLLUP Operator The ROLLUP operator can also be used to calculate sub-totals for each column, based on the groupings within that column. 
 
	Let’s look at an example where we want the sum of employee salaries at a department and gender level along with a sub-total along with a grand total for all salaries of all male and female employees belonging to all departments`. 
 
	This query returns the table below. As you can see it returns the sum of the salaries of the employees of each department divided into three categories: Male, Female and All Genders. The sub-totals are the lines with All in them. The last line is the grand total and so has an All in both columns. 
			
	
	Cube : 
	Cube gives another layer of granuality 
	
	pivot : 
	It is used on Dataset for opertaions which can be achived with rollup and cube 