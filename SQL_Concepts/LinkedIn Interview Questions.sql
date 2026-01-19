**Random SQL Questions Asked in 1-3 YOE Interview**

--Generate unique non-repeating pairs like 'A-B' from a given table using SQL.
--use self join, then on condition should be t1 < t2,it will ensure A<B.Order by will put them in sequence.
select t1.name as first, t2.name as second from items t1
join items t2
on t1.name < t2.name
order by t1.name
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to get a list of top 10 titles that are not in the user titles table
--Use NOT IN operator.
SELECT title
FROM all_titles
WHERE title NOT IN (SELECT title FROM user_titles)
ORDER BY title -- or another column, like `popularity` if available
LIMIT 10;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to get the average renewal time for each user, using start date and end date columns. 
--use avg(date_diff()) to get average renewal time , group by and order by user_id
select id,avg(date_diff(start_date,end_date)) as renewal_time
from users_renewal group by id order by id;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write an SQL query to get the number of employees working in each department (Employees and Department tables given). 
--use left join to combine tables, take count of employee_id's and group by department_name.
select d.department_name,count(e.employee_id) as employee_count from departments d
left join employees e
on d.department_id=e.department_id
group by department_name
employee_count DESC;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write an SQL query to get the number of employees present in the Employees table and not in the Employees backup table.
--this can be solved using not exists/not in/left join. We will go with NOT IN.
--take count of all employees and use not in to filter out results.
SELECT COUNT(*) AS missing_employee_count
FROM Employees
WHERE employee_id NOT IN (
    SELECT employee_id FROM Employees_backup);
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SQL query to find the number of customers who bought an iPhone from a store located in Mumbai in the last month.
--join the tables first, then filter with conditions using where clause.
select count(distinct s.customer_id) as customer_count from sales s
join store st
on s.store_id=st.store_id
where s.product_name = 'iPhone'
AND st.city = 'Mumbai'
AND s.sale_date >= DATEADD(MONTH, -1, CURRENT_DATE) -- Adjust for SQL dialect
AND s.sale_date < CURRENT_DATE;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to find non-repeating values from two SQL tables with sets {1, 2, 3, 4, 5, 6} and {4, 5, 6, 7}. 
--Two ways to solve this 1: using except or minus with union, 2:using left and right join with union

--1st solution:EXCEPT: Returns rows from the first query that are not in the second query.
select value from table1
except 						--Finds values in Table1 but not in Table2.
select value from table2
union
select value from table2
except
select value from table1

--2nd solution: Right and Left join
select t1.value from table1 t1
left join table2 t2 on t1.value=t2.value
WHERE t2.value IS NULL 						--finds values in Table1 but not in Table2
union
select t2.value from table1 t2
left join table1 t2 on t1.value=t2.value
WHERE t1.value IS NULL
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--How to calculate the variation in sales from yesterday to today in SQL?
with daily_sales as (				--Aggregates the total sales (SUM(sale_amount)) for each date
    select sale_date,sum(sale_amount) as total_sales
    from sales
    where sale_date >= current_date - interval 1 day -- include today and yesterday
    group by sale_date
),
sales_variation as (				--Joins daily_sales on today and yesterday’s dates to align their totals.
    select ds1.total_sales as yesterday_sales,ds2.total_sales as today_sales
    from daily_sales ds1
    join daily_sales ds2
    on ds1.sale_date = current_date - interval 1 day and ds2.sale_date = current_date
)
select today_sales,yesterday_sales,
(today_sales - yesterday_sales) as absolute_variation,
((today_sales - yesterday_sales) * 100.0 / nullif(yesterday_sales, 0)) as percentage_variation
from sales_variation;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--How would you find departments that have no employees?
--you can use a LEFT JOIN or a NOT EXISTS approach to identify departments in the Departments table that have no matching entries in the Employees table.
select d.department_id,d.department_name
from departments d
left join employees e on d.department_id = e.department_id
where e.department_id is null;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--How to pivot quarterly data into a year-wise format using SQL?
--use conditional aggregation to transform rows into columns
select year,
    MAX(CASE WHEN quarter = 'Q1' THEN sales_amount END) AS Q1,
    MAX(CASE WHEN quarter = 'Q2' THEN sales_amount END) AS Q2,
    MAX(CASE WHEN quarter = 'Q3' THEN sales_amount END) AS Q3,
    MAX(CASE WHEN quarter = 'Q4' THEN sales_amount END) AS Q4
from sales
group by year order by year;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--How to restrict inserts in a table to specific cities using SQL?
--CHECK Constraint: Simple and effective for most cases but less flexible for complex validation logic.
--Trigger: More versatile but slightly more overhead and database-system dependent.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------