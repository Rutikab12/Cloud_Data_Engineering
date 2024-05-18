WITH Clause

it is a common table expression

--fetch employees who earn more than average salary of all employees

with average_salary (avg_sal) as
	(select cast(avg(salary) as int) from employee)
	
select * from employee e, average_salary average
where e.salary > av.avg_sal;

--find stores who's sales where beteer than the average sales across all stores

Here we need to find 3 key points
1)Total Sales per each store -- Total_Sales
select store_id , sum(cost) as total_sales_of_stores
from sales group by store_id;

2)Find the avergae sales with respect all the stores --Avg_Sales
select cast(avg(total_sales_of_stores) as int) as avg_sales_for_all_stores from(	
	select store_id , sum(cost) as total_sales_of_stores
	from sales group by store_id) x;

3)Find the stores where Total_Sales > Avg_Sales of all stores

Approach-1:
select *
from (select store_id , sum(cost) as total_sales_of_stores
		from sales group by store_id) as total_sales;
JOIN (select cast(avg(total_sales_of_stores) as int) as 				avg_sales_for_all_stores from(	
		select store_id , sum(cost) as total_sales_of_stores
		from sales group by store_id) avg_sales;
ON total_sales.total_sales_of_stores > avg_sales.avg_sales_for_all_stores;

this approaches uses nested queries and hence not easy to READ

Approach-2:
with Total_Sales (store_id,total_sales_of_stores) as
	(select store_id , sum(cost) as total_sales_of_stores
	from sales group by store_id),
	avg_sales (avg_sales_for_all_stores) as
	(select cast(avg(total_sales_of_stores) as int) as 				avg_sales_for_all_stores from Total_Sales)
	--here total_sales is directly used cause sql will create total_Sales table first hence we can use it in next statement

select *
from Total_Sales ts
join avg_sales av
on ts.total_sales_of_stores > a.avg_sales_for_all_stores;
