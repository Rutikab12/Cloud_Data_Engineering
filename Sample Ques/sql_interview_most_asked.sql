--unified-canyon-477712-r8.bq_stage
select * from bq_stage.orders

select * from bq_stage.order_items

select * from bq_stage.customers


--1. Latest order per customer (ROW_NUMBER + QUALIFY)
with cte as(
  select *,row_number() over(partition by customer_id order by order_date desc, order_id desc) as rn
  from bq_stage.orders
)
select order_id,customer_id,order_date from cte where rn=1;


--2. Top 3 orders by amount per customer (DENSE_RANK)
select order_id, customer_id, amount from
(
  select *, dense_rank() over(partition by customer_id order by amount desc) as drn from bq_stage.orders
)
where drn<4;

--3. Day-over-day change in spend (LAG)
with cte as(
  select order_date, sum(amount) as daily_sales,
  from bq_stage.orders
  group by order_date
)
select order_date, daily_sales,lag(daily_sales) OVER (ORDER BY order_date) as prev_sale,
daily_sales-lag(daily_sales) OVER (ORDER BY order_date) as delta
from cte
order by order_date

--4. Running Total per customer
select customer_id, order_id, order_date, amount,
sum(amount) over (partition by customer_id order by order_date, order_id
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_spend
from bq_stage.orders;

--5. 7-day rolling sales (moving window)
select order_date,sum(amount) as sales,
sum(sum(amount)) over(order by order_date RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) as sales7_day
from bq_stage.orders
group by order_date
order by order_date;

--------------------------------------------------------------------------------------------------------------------
--select last 30 days orders
select * from bq_stage.orders
where order_date >= DATE_SUB('2024-04-15',INTERVAL 30 DAY);

--2. Orders per month (DATE_TRUNC) --gives number value of month
select date_trunc(order_date, MONTH) as month,count(order_id) as cnt
from bq_stage.orders
group by month
order by month

--3. age of customer account(date_diff)
select *, date_diff(current_date(),date(signup_ts),DAY) As diff
from bq_stage.customers

--4. Week number & ISO week-year (EXTRACT)
select order_id,extract(ISOWEEK from order_date) as week,
extract(ISOYEAR from order_date) as year
from bq_stage.orders

--5. First day of previous month
select date_trunc(date_sub(current_date(),interval 1 MONTH),MONTH) as prev_month;

--6. last day of current month
SELECT DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS last_day_curr_month;

--7. Convert TIMESTAMP to IST date (Asia/Kolkata)
SELECT DATE(TIMESTAMP_TRUNC(signup_ts, DAY, 'Asia/Kolkata')) AS ist_date
FROM bq_stage.customers;

--8. Monthly retention: customers who ordered in consecutive months
with m as(
select customer_id,date_trunc(order_date,Month) as Month
from `bq_stage.orders`
order by 1,2),
next_m as(
  SELECT customer_id, month, DATE_ADD(month, INTERVAL 1 MONTH) AS next_month
  FROM m
)
select count(*) as retained
FROM next_m n
JOIN m m2 ON n.customer_id = m2.customer_id AND n.next_month = m2.month;

--9. Orders between two dates (inclusive)
SELECT *
FROM bq_stage.orders
WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-03-31';

--10. Bucket orders into fiscal quarters (starting in Apr)
SELECT order_id, order_date,
  CONCAT('FY', EXTRACT(YEAR FROM DATE_ADD(order_date, INTERVAL 9 MONTH)), '-Q',
         CAST(((EXTRACT(MONTH FROM DATE_ADD(order_date, INTERVAL 9 MONTH)) - 1) / 3) + 1 AS INT64)) AS fiscal_qtr
FROM bq_stage.orders;
----------------------------------------------------------------------------------------------------------------------------
--✅ 1. Top-N by Group (Most Asked)

--Find the top 3 highest spending orders per customer.
select order_id, customer_id, order_date, amount
from (
  select o.*,
         row_number() over (partition by customer_id order by amount desc, order_date desc) as rn
  from bq_stage.orders o
) t
where rn <= 3
order by customer_id, amount desc;

--Retrieve the latest order per product based on order_date.
select product_id, product_name, order_id, order_date
from (
  select p.product_id,
         p.product_name,
         o.order_id,
         o.order_date,
         row_number() over (partition by p.product_id order by o.order_date desc, o.order_id desc) as rn
  from bq_stage.products p
  join bq_stage.order_items oi on p.product_id = oi.product_id
  join bq_stage.orders o      on oi.order_id = o.order_id
) t
where rn = 1;


--Get the top 2 employees with the highest salary per department.
select emp_name,emp_id,salary,department from(
select *,rank() over(partition by department order by salary desc) as rnk
from `bq_stage.employees`)a
where rnk<=2
ORDER BY department, salary DESC;

--Find the top 1 most ordered product per month.
with monthly_sales as (
  select
    date_trunc(o.order_date, month) as month_start,  -- use trunc to month
    p.product_id,
    p.product_name,
    sum(oi.quantity) as total_qty
  from bq_stage.orders o
  join bq_stage.order_items oi on o.order_id = oi.order_id
  join bq_stage.products p    on oi.product_id = p.product_id
  group by 1, 2, 3
)
select month_start, product_id, product_name, total_qty
from (
  select ms.*,
         row_number() over (partition by month_start order by total_qty desc) as rn
  from monthly_sales ms
) t
where rn = 1
order by month_start;

--List top 5 customers by transaction amount per city.
with customer_spend as (
  select
    c.city,
    c.customer_id,
    c.customer_name,
    sum(o.amount) as total_spend
  from bq_stage.customers c
  join bq_stage.orders o on c.customer_id = o.customer_id
  group by c.city, c.customer_id, c.customer_name
)
select city, customer_id, customer_name, total_spend
from (
  select cs.*,
         row_number() over (partition by city order by total_spend desc) as rn
  from customer_spend cs
) t
where rn <= 5
order by city, total_spend desc;

-------------------------------------------------------------------------------------------------------------------------
--✅ 2. Window Functions (ROW_NUMBER, RANK, LAG, LEAD)

--Use ROW_NUMBER to remove duplicates and keep the latest record.
select customer_id,customer_name,email,city,signup_date from(
select *,row_number() over(partition by email order by signup_date desc) as rnk
from bq_stage.customers)a
where rnk=1;

--Use RANK to find the 2nd highest salary without using LIMIT or MAX.
select emp_id,salary from (
select *,dense_rank() over(order by salary desc) as rnk from `bq_stage.employees`)a
where rnk=2;

--Show each order with previous order amount (using LAG).
select customer_id,order_id,amount,prev_order from(
select *,lag(amount) over(partition by customer_id order by order_id) as prev_order from bq_stage.orders) a

--Show each transaction with the next transaction date (using LEAD).
select *,lead(order_date) over(partition by customer_id order by order_id) as next_transaction from bq_stage.orders

--Calculate running total of revenue per day (SUM OVER).
select
  order_date,
  sum(amount) as day_total,
  sum(sum(amount)) over (order by order_date
                         rows between unbounded preceding and current row) as cumulative_revenue
from bq_stage.orders
group by order_date
order by order_date;

--Calculate running count of orders per customer.
select
  customer_id, order_id, order_date,
  count(*) over (
    partition by customer_id
    order by order_date, order_id
    rows between unbounded preceding and current row
  ) as orders_so_far
from bq_stage.orders
order by customer_id, order_date;

--Get the difference between consecutive order amounts per customer.
select
  customer_id, order_id, order_date, amount,
  lead(amount) over (partition by customer_id order by order_date, order_id) as next_trans_amt,
  lead(amount) over (partition by customer_id order by order_date, order_id) - amount as diff_to_next
from bq_stage.orders
order by customer_id, order_date;

--Identify customers whose order amount increased compared to previous order.
select customer_id, order_id, order_date, amount, prev_amount
from (
  select *,
         lag(amount) over (partition by customer_id order by order_date, order_id) as prev_amount
  from bq_stage.orders
) t
where prev_amount is not null
  and amount > prev_amount
order by customer_id, order_date;

------------------------------------------------------------------------------------------------------------------------------
--✅ 3. SCD Type-1 vs Type-2 (Very Common Analytics Question)

--1) SCD Type-1 — Overwrite existing rows (no history)
--approach-1:		
	merge into bq_stage.dim_customers_t1 as tgt
	using `bq_stage.customers_stg` as src
	on tgt.customer_id=src.customer_id
	when matched and
	(
	  tgt.customer_name is distinct from src.customer_name or
	  tgt.city is distinct from src.city
	)then
	update set
	  customer_name = src.customer_name,
	  city = src.city,
	  updated_at = src.updated_at
	when not matched then
	insert (customer_id,customer_name,city,updated_at)
	values(src.customer_id, src.customer_name, src.city, src.updated_at);
--approach-2:
	-- 1) update existing rows where attributes changed
	update bq_stage.dim_customers_t1 tgt
	set
	  customer_name = src.customer_name,
	  city          = src.city,
	  updated_at    = src.updated_at
	from bq_stage.customers_stg src
	where tgt.customer_id = src.customer_id
	  and (
		   tgt.customer_name is distinct from src.customer_name
		or tgt.city          is distinct from src.city
	  );

	-- 2) insert new customers not present in target
	insert into bq_stage.dim_customers_t1 (customer_id, customer_name, city, updated_at)
	select src.customer_id, src.customer_name, src.city, src.updated_at
	from bq_stage.customers_stg src
	left join bq_stage.dim_customers_t1 tgt
	on src.customer_id = tgt.customer_id
	where tgt.customer_id is null;

--2)SCD Type-2 — Maintain history (close old row + insert new row)
--Approach-1:
	merge into bq_stage.dim_customers_t2 as tgt
	using (select * from `bq_stage.customers_stg`) as src
	on tgt.customer_id=src.customer_id and tgt.is_current=TRUE
	when matched and
	(
	  tgt.customer_name is distinct from src.customer_name or
	  tgt.city is distinct from src.city
	)then
	update set
	  effective_to = src.updated_at,   -- close at change time (or src.updated_at - small delta)
	  is_current   = FALSE
	when not matched then
	--do nothing we will write separate query

	insert into bq_stage.dim_customers_t2 (customer_key, customer_id, customer_name, city, effective_from, effective_to, is_current, updated_at)
	select
	  nextval('seq_dim_customer_key') as customer_key,  -- if your db has sequence, else omit surrogate or use uuid
	  s.customer_id,
	  s.customer_name,
	  s.city,
	  s.updated_at as effective_from,
	  null        as effective_to,
	  true        as is_current,
	  s.updated_at as updated_at
	from bq_stage.customers_stg s
	left join bq_stage.dim_customers_t2 cur
	  on s.customer_id = cur.customer_id and cur.is_current = true
	where
	-- either new customer (no current) or changed compared to current
	curr.customer_id is null
	or (
		cur.customer_name is distinct from s.customer_name
		or cur.city is distinct from s.city
	);

--Approach-2:no merge
	-- A) Close current rows which are changed
	update bq_stage.dim_customers_t2 tgt
	set effective_to = src.updated_at,
		is_current   = false
	from bq_stage.customers_stg src
	where tgt.customer_id = src.customer_id
	  and tgt.is_current = true
	  and (
		   tgt.customer_name is distinct from src.customer_name
		or tgt.city          is distinct from src.city
	  );

	-- b) insert new rows for new or changed
	insert into bq_stage.dim_customers_t2 (customer_key, customer_id, customer_name, city, effective_from, effective_to, is_current, updated_at)
	select
	  nextval('seq_dim_customer_key'), -- or generate surrogate
	  s.customer_id,
	  s.customer_name,
	  s.city,
	  s.updated_at,
	  null,
	  true,
	  s.updated_at
	from bq_stage.customers_stg s
	left join bq_stage.dim_customers_t2 cur
	on s.customer_id = cur.customer_id and cur.is_current = true
	where cur.customer_id is null
	or (cur.customer_name is distinct from s.customer_name or cur.city is distinct from s.city);
	
--3) Detect new vs changed vs unchanged (comparison query)
select
  s.customer_id,
  s.customer_name  as stg_name,
  s.city           as stg_city,
  cur.customer_name as cur_name,
  cur.city          as cur_city,
  case
    when cur.customer_id is null then 'new'
    when (cur.customer_name is distinct from s.customer_name)
      or (cur.city is distinct from s.city) then 'changed'
    else 'unchanged'
  end as change_type
from customers_stg s
left join dim_customers_t2 cur
  on s.customer_id = cur.customer_id
  and cur.is_current = true;

-------------------------------------------------------------------------------------------------------------------------------------------
--✅ 4. delete duplicates but keep latest

--delete duplicate rows but retain only the latest row based on updated_at.
	select order_id,customer_id,order_date,amount,updated_at from(
	select *,row_number() over(partition by customer_id order by updated_at desc) as rnk
	from bq_stage.customer_orders)a
	where rnk=1

--write sql to list only duplicate rows (not just the values).
	select * from bq_stage.customer_orders where customer_id in(
	select customer_id from bq_stage.customer_orders
	group by customer_id
	having count(*)>1)

--delete duplicate rows based on (email, phone).
	delete from bq_stage.customers
	where customer_id in (
	  select customer_id
	  from (
		select customer_id,
			   row_number() over (partition by email, phone
								  order by signup_date desc, customer_id desc) as rn
		from bq_stage.customers
	  ) x
	  where rn > 1
	);

--keep the earliest row per customer and delete all others.
	delete from bq_stage.customers
	where customer_id in (
	  select customer_id
	  from (
		select customer_id,
			   row_number() over (partition by customer_id order by signup_date asc) as rn
		from bq_stage.customers
	  ) x
	  where rn > 1
	);

--find duplicates in a table without using group by
	select *
	from (
		select *,count(*) over (partition by email) as cnt
		from bq_stage.customers
	) x
	where cnt > 1;

--retrieve duplicate entries and their counts in one query
	select customer_id, count(*) as cnt
	from bq_stage.customers
	group by customer_id
	having count(*) > 1;

--count unique vs duplicate rows
	select
		count(*) as total_rows,
		count(distinct customer_id) as unique_rows,
		count(*) - count(distinct customer_id) as duplicate_rows
	from customers;

--find duplicates across multiple columns
	select customer_name,email, city, count(*)
	from bq_stage.customers
	group by 1, 2, 3
	having count(*) > 1;

-----------------------------------------------------------------------------------------------------------------------------------------------
--✅ 5. Find 2nd / Nth Highest Salary (Classic Questions)

--Find 2nd highest salary using subquery.
Select MAX(salary) as SecondHighestSalary from bq_stage.employees
where salary < (Select MAX(salary) from bq_stage.employees)

--Find 2nd highest salary using window functions.
select emp_id,salary from(
select emp_id,salary,dense_rank() over(order by salary desc) as rn from `bq_stage.employees`)a
where rn=2

--Find Nth highest salary without window functions.
-- N is 1 for highest, 2 for second highest, ...
-- replace :n with an integer or pass as parameter if supported
select distinct salary
from bq_stage.employees
order by salary desc
limit 1 offset :n - 1;

--find employees whose salary equals the maximum in their department.
select emp_id, department, salary
from (
  select emp_id,
         department,
         salary,
         max(salary) over (partition by department) as max_salary_by_dept
  from employees
) t
where salary = max_salary_by_dept;

-------------------------------------------------------------------------------------------------------------------------------------------------
--✅ 6. Rolling Aggregates (7-day / 30-day windows)

--calculate a 7-day rolling total of sales.
with daily as (
  select
    order_date,
    sum(amount) as daily_sales
  from bq_stage.orders
  group by order_date
  order by order_date
)
select
  order_date,
  daily_sales,
  sum(daily_sales) over (
    order by order_date
    rows between 6 preceding and current row   -- 30-row window
  ) as avg_30_row
from daily
order by order_date;

--calculate 30-day moving average of sales.
with daily as (
  select
    order_date,
    sum(amount) as daily_sales
  from bq_stage.orders
  group by order_date
  order by order_date
)
select
  order_date,
  daily_sales,
  avg(daily_sales) over (
    order by order_date
    rows between 29 preceding and current row   -- 30-row window
  ) as avg_30_row
from daily
order by order_date;



--show day-over-day difference in sales (lag).
with daily as (
  select
    order_date,
    sum(amount) as daily_sales
  from bq_stage.orders
  group by order_date
)
select
  order_date,
  daily_sales,
  lag(daily_sales) over (order by order_date) as prev_day_sales,
  coalesce(daily_sales - lag(daily_sales) over (order by order_date), 0) as delta
from daily
order by order_date;


--monthly mtd (month-to-date) sales.
with daily as (
  select
    order_date,
    sum(amount) as daily_sales
  from bq_stage.orders
  group by order_date
)
select
  order_date,
  date_trunc(order_date, month) as month_start,
  daily_sales,
  sum(daily_sales) over (
    partition by date_trunc(order_date, month)
    order by order_date
    rows between unbounded preceding and current row
  ) as mtd_sales
from daily
order by order_date;

----------------------------------------------------------------------------------------------------------------------------------------------
--✅ 7. PIVOT / UNPIVOT

--Pivot daily order counts into columns for Monday–Sunday.
-- Assume table orders(order_id, order_date DATE)
select
  date(order_date) as order_date,
  sum(case when extract(day from order_date) = 1 then 1 else 0 end) as monday_count,
  sum(case when extract(day from order_date) = 2 then 1 else 0 end) as tuesday_count,
  sum(case when extract(day from order_date) = 3 then 1 else 0 end) as wednesday_count,
  sum(case when extract(day from order_date) = 4 then 1 else 0 end) as thursday_count,
  sum(case when extract(day from order_date) = 5 then 1 else 0 end) as friday_count,
  sum(case when extract(day from order_date) = 6 then 1 else 0 end) as saturday_count,
  sum(case when extract(day from order_date) = 0 then 1 else 0 end) as sunday_count
from bq_stage.orders
group by date(order_date)
order by order_date;

--pivot product sales so each product becomes a column
-- example: order_items(order_id, product_id, qty), products(product_id, product_name, price)
select
  oi.order_id,
  sum(case when p.product_name = 'laptop' then oi.quantity * p.price else 0 end) as laptop_sales,
  sum(case when p.product_name = 'headphones' then oi.quantity * p.price else 0 end) as headphones_sales,
  sum(case when p.product_name = 'keyboard' then oi.quantity * p.price else 0 end) as keyboard_sales,
  sum(oi.quantity * p.price) as total_order_revenue
from bq_stage.order_items oi
join bq_stage.products p on oi.product_id = p.product_id
group by oi.order_id;

--unpivot survey response columns into row format
select id, respondent_id, 'q1' as question, q1 as answer from bq_stage.survey
union all
select id, respondent_id, 'q2' as question, q2 as answer from bq_stage.survey
union all
select id, respondent_id, 'q3' as question, q3 as answer from bq_stage.survey;

--convert year-wise columns into a long format using unpivot
select customer_id, 2019 as year, sales_2019 as sales from bq_stage.sales
union all
select customer_id, 2020 as year, sales_2020 as sales from bq_stage.sales
union all
select customer_id, 2021 as year, sales_2021 as sales from bq_stage.sales;
