--Given the Orders table with columns OrderID, 
--OrderDate, and TotalAmount, and the 
--Returns table with columns ReturnID and OrderID, 

--write an SQL query to calculate the total 
--numbers of returned orders for each month

drop table if exists cricket_dataset.orders;
CREATE TABLE cricket_dataset.orders (
    OrderID INT64,
    OrderDate DATE,
    TotalAmount float64
);

DROP TABLE IF EXISTS cricket_dataset.returns;
CREATE TABLE cricket_dataset.returns (
    ReturnID INT64,
    OrderID INT64,
);

INSERT INTO cricket_dataset.orders (OrderID, OrderDate, TotalAmount) VALUES
(1, '2023-01-15', 150.50),
(2, '2023-02-20', 200.75),
(3, '2023-02-28', 300.25),
(4, '2023-03-10', 180.00),
(5, '2023-04-05', 250.80);

INSERT INTO cricket_dataset.returns (ReturnID, OrderID) VALUES
(101, 2),
(102, 4),
(103, 5),
(104, 1),
(105, 3);

select * from cricket_dataset.orders;

select * from cricket_dataset.returns;

--appraoch is first extract month, then take count of returns and do left join on returns as we need that data only

select extract(Month from a.OrderDate) as month,count(b.ReturnID) as count_of_return
from cricket_dataset.returns b
left join cricket_dataset.orders a
on b.OrderID=a.OrderID
group by month
------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write SQL query to find the top-selling products in each category

DROP TABLE IF EXISTS cricket_dataset.products;
CREATE TABLE cricket_dataset.products (
    product_id INT64,
    product_name string,
    category string,
    quantity_sold INT64
);

-- Step 2: Insert sample records into the products table
INSERT INTO cricket_dataset.products (product_id, product_name, category, quantity_sold) VALUES
(1, 'Samsung Galaxy S20', 'Electronics', 100),
(2, 'Apple iPhone 12 Pro', 'Electronics', 150),
(3, 'Sony PlayStation 5', 'Electronics', 80),
(4, 'Nike Air Max 270', 'Clothing', 200),
(5, 'Adidas Ultraboost 20', 'Clothing', 200),
(6, 'Levis Mens 501 Jeans', 'Clothing', 90),
(7, 'Instant Pot Duo 7-in-1', 'Home & Kitchen', 180),
(8, 'Keurig K-Classic Coffee Maker', 'Home & Kitchen', 130),
(9, 'iRobot Roomba 675 Robot Vacuum', 'Home & Kitchen', 130),
(10, 'Breville Compact Smart Oven', 'Home & Kitchen', 90),
(11, 'Dyson V11 Animal Cordless Vacuum', 'Home & Kitchen', 90);

select * from cricket_dataset.products

--approach is using window function, order by quantities sold but using rank() to get of one item have sold same units, and not row_number() as it will avoid that.

with cte as(
select *,
rank() over(partition by category order by quantity_sold desc) as rn
from cricket_dataset.products
order by quantity_sold,category desc
)
select * from cte where rn=1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Find the top 2 products in the top 2 categories based on spend amount
--find top 2 categories based on spends
drop table if exists cricket_dataset.orders;
create table cricket_dataset.orders(
  	category string,
	  product string,
	  user_id int64 , 
  	spend float64,
  	transaction_date DATE
);

Insert into cricket_dataset.orders values
('appliance','refrigerator',165,246.00,'2021-12-26'),
('appliance','refrigerator',123,299.99,'2022-03-02'),
('appliance','washingmachine',123,219.80,'2022-03-02'),
('electronics','vacuum',178,152.00,'2022-04-05'),
('electronics','wirelessheadset',156,	249.90,'2022-07-08'),
('electronics','TV',145,189.00,'2022-07-15'),
('Television','TV',165,129.00,'2022-07-15'),
('Television','TV',163,129.00,'2022-07-15'),
('Television','TV',141,129.00,'2022-07-15'),
('toys','Ben10',145,189.00,'2022-07-15'),
('toys','Ben10',145,189.00,'2022-07-15'),
('toys','yoyo',165,129.00,'2022-07-15'),
('toys','yoyo',163,129.00,'2022-07-15'),
('toys','yoyo',141,129.00,'2022-07-15'),
('toys','yoyo',145,189.00,'2022-07-15'),
('electronics','vacuum',145,189.00,'2022-07-15');

select * from cricket_dataset.orders

--first find highest spend categories and rank them ,then take top 2 from them
select category,total_spend_on_category from
(
	select category,sum(spend) as total_spend_on_category,
	dense_rank() over(order by sum(spend) desc) as rn
	from cricket_dataset.orders
	group by category
)
where rn<=2

--2nd solution, where we will first find top products in each category and then join with top2 categories
--to get the top2 products in top2 categories by spend amount
with ranked_category as(
	select category,total_spend_on_category from
	(
		select category,sum(spend) as total_spend_on_category,
		dense_rank() over(order by sum(spend) desc) as rn
		from cricket_dataset.orders
		group by category
	) as subquery1
	where rn<=2
)
select category,product,total_spend_product_wise from
(
	select d.category,d.product,sum(d.spend) as total_spend_product_wise,
	dense_rank() over(partition by d.category order by sum(d.spend)desc) as drn
	from cricket_dataset.orders d
	join ranked_category r
	on d.category=r.category
	group by d.product,d.category
)subsquery2
where drn<=2
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--retrieve the third highest salary from the Employee table.

DROP TABLE IF EXISTS cricket_dataset.employees;
-- Create the Employee table
CREATE TABLE cricket_dataset.employees (
    EmployeeID INT64,
    Name string,
    Department string,
    Salary float64,
    HireDate DATE
);

-- Insert sample records into the Employee table
INSERT INTO cricket_dataset.employees (EmployeeID, Name, Department, Salary, HireDate) VALUES
(101, 'John Smith', 'Sales', 60000.00, '2022-01-15'),
(102, 'Jane Doe', 'Marketing', 55000.00, '2022-02-20'),
(103, 'Michael Johnson', 'Finance', 70000.00, '2021-12-10'),
(104, 'Emily Brown', 'Sales', 62000.00, '2022-03-05'),
(106, 'Sam Brown', 'IT', 62000.00, '2022-03-05'),	
(105, 'Chris Wilson', 'Marketing', 58000.00, '2022-01-30');

select * from cricket_dataset.employees;

--use dense_rank to get top3 salary

SELECT 
	salary as third_highest_salary
FROM
(	SELECT 
		*,
		DENSE_RANK() OVER( ORDER BY salary desc) drn
	FROM cricket_dataset.employees
) as subquery	
WHERE drn = 3
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write an SQL query to find customers who haven't made any purchases in the last month, 
--assuming today's date is April 2, 2024.

DROP TABLE IF EXISTS cricket_dataset.customers;
CREATE TABLE cricket_dataset.customers (
    customer_id INT64,
    name string,
    email string
);

DROP TABLE IF EXISTS cricket_dataset.orders;
CREATE TABLE cricket_dataset.orders (
    order_id INT64,
    customer_id INT64,
    order_date DATE,
    amount float64
);

-- Inserting sample customers
INSERT INTO cricket_dataset.customers (customer_id, name, email) VALUES
(1, 'John Doe', 'john@example.com'),
(2, 'Jane Smith', 'jane@example.com'),
(3, 'Alice Johnson', 'alice@example.com'),
(4, 'Sam B', 'sb@example.com'),
(5, 'John Smith', 'j@example.com')	
;

-- Inserting sample orders
INSERT INTO cricket_dataset.orders (order_id, customer_id, order_date, amount) VALUES
(1, 1, '2024-06-05', 50.00),
(2, 2, '2024-06-10', 75.00),
(5, 4, '2024-07-02', 45.00),
(5, 2, '2024-07-02', 45.00)	,
(3, 4, '2024-07-15', 100.00),
(4, 1, '2024-07-01', 60.00),
(5, 5, '2024-07-02', 45.00);

select * from cricket_dataset.orders;

select * from cricket_dataset.customers;

--approach was to extract current month and year, also previous month only and then check whether it have entry in orders table or not.

SELECT *
FROM cricket_dataset.customers
WHERE customer_id NOT IN (SELECT customer_id FROM cricket_dataset.orders
    WHERE EXTRACT(MONTH from order_date) 
    = EXTRACT(MONTH FROM current_date)-1 	
    AND 
    EXTRACT(YEAR FROM order_date) = 
    EXTRACT(YEAR FROM current_date)
);

------------------------------------------------------------------------------------------------------------------------------------------------------------------
--find all products that haven't been sold in the last six months. 
--Return the product_id, product_name, category, and price of these products.

DROP TABLE IF EXISTS cricket_dataset.products;
CREATE TABLE cricket_dataset.products (
    product_id int64,
    product_name string,
    category string,
    price float64
);

-- Insert sample records into Product table
INSERT INTO cricket_dataset.products (product_id,product_name, category, price) VALUES
(1,'Product A', 'Category 1', 10.00),
(2,'Product B', 'Category 2', 15.00),
(3,'Product C', 'Category 1', 20.00),
(4,'Product D', 'Category 3', 25.00);


-- Create Sales table
DROP TABLE IF EXISTS cricket_dataset.sales;
CREATE TABLE cricket_dataset.sales (
    sale_id int64,
    product_id INT64,
    sale_date DATE,
    quantity INT64,
);

-- Insert sample records into Sales table
INSERT INTO cricket_dataset.sales (sale_id,product_id, sale_date, quantity) VALUES
(1,1, '2023-09-15', 5),
(2,2, '2023-10-20', 3),
(3,1, '2024-01-05', 2),
(4,3, '2024-02-10', 4),
(5,4, '2023-12-03', 1);

select * from cricket_dataset.products;

select * from cricket_dataset.sales;

--do left join as we need all details from product table and extract current_date - interval of 6 months
--sales_id is null, will give us products not sold before

select p.*,s.sale_date
from cricket_dataset.products p
left join cricket_dataset.sales s
on p.product_id=s.product_id
where s.sale_date is null or
s.sale_date < date_sub(date_trunc(current_date() ,Month),interval 6 Month)

--select all product which has not received any sale in current year
select p.product_id, p.product_name, p.category,  p.price from cricket_dataset.products as p where p.product_id Not in(
select product_id from cricket_dataset.sales where extract(year from sale_date)=2024 )
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--write a SQL query to find customers who  bought Airpods after purchasing an iPhone.
DROP TABLE IF EXISTS cricket_dataset.customers;
CREATE TABLE cricket_dataset.customers (
    CustomerID INT64,
    CustomerName string
);

-- Create Purchases table
DROP TABLE IF EXISTS cricket_dataset.purchases;
CREATE TABLE cricket_dataset.purchases (
    PurchaseID INT64,
    CustomerID INT64,
    ProductName string,
    PurchaseDate DATE
);

-- Insert sample data into Customers table
INSERT INTO cricket_dataset.customers (CustomerID, CustomerName) VALUES
(1, 'John'),
(2, 'Emma'),
(3, 'Michael'),
(4, 'Ben'),
(5, 'John')	;

-- Insert sample data into Purchases table
INSERT INTO cricket_dataset.purchases (PurchaseID, CustomerID, ProductName, PurchaseDate) VALUES
(100, 1, 'iPhone', '2024-01-01'),
(101, 1, 'MacBook', '2024-01-20'),	
(102, 1, 'Airpods', '2024-03-10'),
(103, 2, 'iPad', '2024-03-05'),
(104, 2, 'iPhone', '2024-03-15'),
(105, 3, 'MacBook', '2024-03-20'),
(106, 3, 'Airpods', '2024-03-25'),
(107, 4, 'iPhone', '2024-03-22'),	
(108, 4, 'Airpods', '2024-03-29'),
(110, 5, 'Airpods', '2024-02-29'),
(109, 5, 'iPhone', '2024-03-22');

select * from cricket_dataset.purchases;

select * from `cricket_dataset.customers`;


--first find the customers who bought iphones
--All customers who bought Airpods
-- Customer has to buy Airpods after purchasing the iPhone

select distinct c.*
from cricket_dataset.customers c
join cricket_dataset.purchases p1
on c.CustomerID=p1.CustomerID
join cricket_dataset.purchases p2   --self join, one table contains data of iphones bought and other contains data of airpods bought
on c.CustomerID=p2.CustomerID
where p1.ProductName='iPhone' and p2.ProductName='Airpods'
and p1.PurchaseDate < p2.PurchaseDate


--% of chance that customer who bough macbook will buy airpods
With macbook_customers AS (
  SELECT DISTINCT CustomerID   --first find customers who bought macbook
  FROM cricket_dataset.purchases
  WHERE ProductName = 'MacBook'
)
SELECT
  ROUND(
    100.0 * COUNT(CASE WHEN p.ProductName = 'Airpods' THEN 1 END) / COUNT(*),  --take count of airpods and count of all products
    2
  ) AS percentage_chance
FROM macbook_customers mc
LEFT JOIN cricket_dataset.purchases p ON mc.CustomerID = p.CustomerID;  --do left join on with clause data
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a SQL query to classify employees into three categories based on their salary:

--"High" - Salary greater than $70,000
--"Medium" - Salary between $50,000 and $70,000 (inclusive)
--"Low" - Salary less than $50,000

--Your query should return the EmployeeID, FirstName, LastName, Department, Salary, and a new column SalaryCategory indicating the category to which each employee belongs


DROP TABLE IF EXISTS cricket_dataset.employees;

CREATE TABLE cricket_dataset.employees (
    EmployeeID INT64,
    FirstName string,
    LastName string,
    Department string,
    Salary float64
);

-- Insert sample records into Employee table
INSERT INTO cricket_dataset.employees (EmployeeID, FirstName, LastName, Department, Salary) VALUES
(1, 'John', 'Doe', 'Finance', 75000.00),
(2, 'Jane', 'Smith', 'HR', 60000.00),
(3, 'Michael', 'Johnson', 'IT', 45000.00),
(4, 'Emily', 'Brown', 'Marketing', 55000.00),
(5, 'David', 'Williams', 'Finance', 80000.00),
(6, 'Sarah', 'Jones', 'HR', 48000.00),
(7, 'Chris', 'Taylor', 'IT', 72000.00),
(8, 'Jessica', 'Wilson', 'Marketing', 49000.00);


--solution with count of employees category wise
with cte as(
SELECT *,
	CASE 
		WHEN salary > 70000 THEN 'High'
		WHEN salary BETWEEN 50000 AND 70000 THEN 'Medium'
		ELSE 'Low'
	END as salary_category
FROM cricket_dataset.employees)
select salary_category, count(*) as employee_count  --count(*) and count(EmployeeID) gives the same result
from cte
group by salary_category;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Identify returning customers based on their order history. Categorize customers as "Returning" if they have placed more than one return, and as "New" otherwise. 
--Considering you have two table orders has information about sale and returns has information about returns 

DROP TABLE IF EXISTS cricket_dataset.orders;
DROP TABLE IF EXISTS cricket_dataset.returns;


-- Create the orders table
CREATE TABLE cricket_dataset.orders (
    order_id string,
    customer_id string,
    order_date DATE,
    product_id string,
    quantity INT64
);

-- Create the returns table
CREATE TABLE cricket_dataset.returns (
    return_id string,
    order_id string
);


INSERT INTO cricket_dataset.orders (order_id, customer_id, order_date, product_id, quantity)
VALUES
    ('1001', 'C001', '2023-01-15', 'P001', 4),
    ('1002', 'C001', '2023-02-20', 'P002', 3),
    ('1003', 'C002', '2023-03-10', 'P003', 8),
    ('1004', 'C003', '2023-04-05', 'P004', 2),
    ('1005', 'C004', '2023-05-20', 'P005', 3),
    ('1006', 'C002', '2023-06-15', 'P001', 6),
    ('1007', 'C003', '2023-07-20', 'P002', 1),
    ('1008', 'C004', '2023-08-10', 'P003', 2),
    ('1009', 'C005', '2023-09-05', 'P002', 3),
    ('1010', 'C001', '2023-10-20', 'P002', 1);

-- Insert sample records into the returns table
INSERT INTO cricket_dataset.returns (return_id, order_id)
VALUES
    ('R001', '1001'),
    ('R002', '1002'),
    ('R003', '1005'),
    ('R004', '1008'),
    ('R005', '1007');


select * from cricket_dataset.orders;

select * from cricket_dataset.returns;

--find total returns and orders by each customers
--count(returns)>1 keeps those records and give them tags as mentioned
with cte as(
select o.customer_id,
count(o.order_id) as total_orders,
count(r.return_id) as total_returns,
--case when count(r.return_id)>1 then 'Returning' else 'New' end
from cricket_dataset.orders o
left join cricket_dataset.returns r
on o.order_id=r.order_id
group by o.customer_id)
select *,case when cte.total_returns>1 then 'Returning' else 'New' end as tagging
from cte
----------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a solution to show the unique ID of each user, 
--If a user does not have a unique ID replace just show null.

DROP TABLE IF EXISTS cricket_dataset.employees;
-- Create Employees table
CREATE TABLE cricket_dataset.employees (
    id INT64,
    name string
);

-- Insert sample data into Employees table
INSERT INTO cricket_dataset.employees (id, name) VALUES
    (1, 'Alice'),
    (7, 'Bob'),
    (11, 'Meir'),
    (90, 'Winston'),
    (3, 'Jonathan');


DROP TABLE IF EXISTS cricket_dataset.employeeuni;
-- Create EmployeeUNI table
CREATE TABLE cricket_dataset.employeeuni (
    id INT64,
    unique_id INT64
);

-- Insert sample data into EmployeeUNI table
INSERT INTO cricket_dataset.employeeuni (id, unique_id) VALUES
    (3, 1),
    (11, 2),
    (90, 3);


--solve using left join

select e.name,u.unique_id from `cricket_dataset.employees` e
left join cricket_dataset.employeeuni u
on e.id=u.id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--write a SQL query to retrieve all employees' details along with their manager's names based on the manager ID

DROP TABLE IF EXISTS cricket_dataset.employees;
CREATE TABLE cricket_dataset.employees (
    emp_id INT64,
    name string,
    manager_id INT64,
);

INSERT INTO cricket_dataset.employees (emp_id, name, manager_id) VALUES
(1, 'John Doe', NULL),        -- John Doe is not a manager
(2, 'Jane Smith', 1),          -- Jane Smith's manager is John Doe
(3, 'Alice Johnson', 1),       -- Alice Johnson's manager is John Doe
(4, 'Bob Brown', 3),           -- Bob Brown's manager is Alice Johnson
(5, 'Emily White', NULL),      -- Emily White is not a manager
(6, 'Michael Lee', 3),         -- Michael Lee's manager is Alice Johnson
(7, 'David Clark', NULL),      -- David Clark is not a manager
(8, 'Sarah Davis', 2),         -- Sarah Davis's manager is Jane Smith
(9, 'Kevin Wilson', 2),        -- Kevin Wilson's manager is Jane Smith
(10, 'Laura Martinez', 4);     -- Laura Martinez's manager is Bob Brown

--use cross join

SELECT    
    e1.emp_id,
    e1.name,
    e1.manager_id,
    e2.name as manager_name
FROM cricket_dataset.employees as e1
CROSS JOIN 
cricket_dataset.employees as e2    
WHERE e1.manager_id = e2.emp_id

--Write a SQL query to find the names of all employees who are also managers. 
select distinct(e2.name),e2.emp_id from cricket_dataset.employees e1
inner join cricket_dataset.employees e2
on e1.manager_id = e2.emp_id;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Find the top 2 customers who have spent the most money across all their orders. 
--Return their names, emails, and total amounts spent.

DROP TABLE IF EXISTS cricket_dataset.customers;
CREATE TABLE cricket_dataset.customers (
   customer_id INT64,
    customer_name string,
    customer_email string
);


DROP TABLE IF EXISTS cricket_dataset.orders;
CREATE TABLE cricket_dataset.orders (
    order_id INT64,
    customer_id INT64,
    order_date DATE,
    order_amount float64
);


INSERT INTO cricket_dataset.customers (customer_id, customer_name, customer_email) VALUES
(1, 'John Doe', 'john@example.com'),
(2, 'Jane Smith', 'jane@example.com'),
(3, 'Alice Johnson', 'alice@example.com'),
(4, 'Bob Brown', 'bob@example.com');

INSERT INTO cricket_dataset.orders (order_id, customer_id, order_date, order_amount) VALUES
(1, 1, '2024-01-03', 50.00),
(2, 2, '2024-01-05', 75.00),
(3, 1, '2024-01-10', 25.00),
(4, 3, '2024-01-15', 60.00),
(5, 2, '2024-01-20', 50.00),
(6, 1, '2024-02-01', 100.00),
(7, 2, '2024-02-05', 25.00),
(8, 3, '2024-02-10', 90.00),
(9, 1, '2024-02-15', 50.00),
(10, 2, '2024-02-20', 75.00);

select * from cricket_dataset.orders;

select * from cricket_dataset.customers;

--take sum of order_amount and do left join and group by and order by total_spend.
select c.customer_id,c.customer_name,c.customer_email,sum(o.order_amount) as total_spend
from cricket_dataset.customers c
left join cricket_dataset.orders o
on c.customer_id=o.customer_id
group by 1,2,3
order by total_spend desc
limit 2

-- customers details who has placed highest orders and total count of orders and total order amount
select c.customer_id,c.customer_name,c.customer_email,sum(o.order_amount) as total_spend,count(o.order_id) as no_of_orders
from cricket_dataset.customers c
left join cricket_dataset.orders o
on c.customer_id=o.customer_id
group by 1,2,3
order by no_of_orders desc
limit 2
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write an SQL query to retrieve the product details for items whose revenue 
--decreased compared to the previous month. 

DROP TABLE IF EXISTS cricket_dataset.orders;
CREATE TABLE cricket_dataset.orders (
    order_id int64,
    order_date DATE,
    product_id INT64,
    quantity INT64,
    price float64
);

-- Inserting records for the current month
INSERT INTO cricket_dataset.orders (order_id,order_date, product_id, quantity, price) VALUES
    (1,'2024-07-01', 1, 10, 50.00),
    (2,'2024-07-02', 2, 8, 40.00),
    (3,'2024-07-03', 3, 15, 30.00),
    (4,'2024-07-04', 4, 12, 25.00),
    (5,'2024-07-05', 5, 5, 60.00),
    (6,'2024-07-06', 6, 20, 20.00),
    (7,'2024-07-07', 7, 18, 35.00),
    (8,'2024-07-08', 8, 14, 45.00),
    (9,'2024-07-09', 1, 10, 50.00),
    (10,'2024-07-10', 2, 8, 40.00);

-- Inserting records for the last month
INSERT INTO cricket_dataset.orders (order_id,order_date, product_id, quantity, price) VALUES
    (11,'2024-06-01', 1, 12, 50.00),
    (12,'2024-06-02', 2, 10, 40.00),
    (13,'2024-06-03', 3, 18, 30.00),
    (14,'2024-06-04', 4, 14, 25.00),
    (15,'2024-06-05', 5, 7, 60.00),
    (16,'2024-06-06', 6, 22, 20.00),
    (17,'2024-06-07', 7, 20, 35.00),
    (18,'2024-06-08', 8, 16, 45.00),
    (19,'2024-06-09', 1, 12, 50.00),
    (20,'2024-06-10', 2, 10, 40.00);

-- Inserting records for the previous month
INSERT INTO cricket_dataset.orders (order_id,order_date, product_id, quantity, price) VALUES
    (21,'2024-05-01', 1, 15, 50.00),
    (22,'2024-05-02', 2, 12, 40.00),
    (23,'2024-05-03', 3, 20, 30.00),
    (24,'2024-05-04', 4, 16, 25.00),
    (25,'2024-05-05', 5, 9, 60.00),
    (26,'2024-05-06', 6, 25, 20.00),
    (27,'2024-05-07', 7, 22, 35.00),
    (28,'2024-05-08', 8, 18, 45.00),
    (29,'2024-05-09', 1, 15, 50.00),
    (30,'2024-05-10', 2, 12, 40.00);

select * from cricket_dataset.orders;

--first find total sales with respect to current_month
--then find total sales with repsect to previous month
--do inner join of both
--apply condition where revenue has decreased of current_month product_ids
with current_month_cte as(
select product_id,sum(price*quantity) as curr_total_sales,sum(quantity) as curr_total_quantity
from `cricket_dataset.orders`
where extract(Month from order_date) = extract(Month from current_date)
group by product_id),
previous_month_cte as(
select product_id,sum(price*quantity) as prev_total_sales,sum(quantity) as prev_total_quantity
from `cricket_dataset.orders`
where extract(Month from order_date) = extract(Month from current_date)-1
group by product_id
)
select c.product_id,
c.curr_total_quantity,c.curr_total_sales,p.prev_total_quantity,p.prev_total_sales
from current_month_cte c
join previous_month_cte p
on c.product_id=p.product_id
where c.curr_total_sales < p.prev_total_sales


--SQL query to find the products whose total revenue has decreased by more than 10% from the previous month to the current month.
with current_month_cte as(
select product_id,sum(price*quantity) as curr_total_sales,sum(quantity) as curr_total_quantity
from `cricket_dataset.orders`
where extract(Month from order_date) = extract(Month from current_date)
group by product_id),
previous_month_cte as(
select product_id,sum(price*quantity) as prev_total_sales,sum(quantity) as prev_total_quantity
from `cricket_dataset.orders`
where extract(Month from order_date) = extract(Month from current_date)-1
group by product_id
)
select c.product_id,
c.curr_total_quantity,c.curr_total_sales,p.prev_total_quantity,p.prev_total_sales,round((p.prev_total_sales-c.curr_total_sales)*100.00/p.prev_total_sales,2) as perc_dec
from current_month_cte c
join previous_month_cte p
on c.product_id=p.product_id
where c.curr_total_sales < p.prev_total_sales
and (p.prev_total_sales-c.curr_total_sales)*100.00/p.prev_total_sales > 10
------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a SQL query to find the names of managers who have at least five direct reports. Return the result table in any order.

DROP TABLE IF EXISTS cricket_dataset.employees;
CREATE TABLE cricket_dataset.employees (
    id INT64,
    name string,
    department string,
    managerId INT64
);

INSERT INTO cricket_dataset.employees (id, name, department, managerId) VALUES
(101, 'John', 'A', NULL),
(102, 'Dan', 'A', 101),
(103, 'James', 'A', 101),
(104, 'Amy', 'A', 101),
(105, 'Anne', 'A', 101),
(106, 'Ron', 'B', 101),
(107, 'Michael', 'C', NULL),
(108, 'Sarah', 'C', 107),
(109, 'Emily', 'C', 107),
(110, 'Brian', 'C', 107);
--first find manager_name based on managerId, then count of emp reporting to that Id, do self join and add condition count<=5
--do self join
select e1.managerId,e2.name as manager_name,count(e1.id) as total_count
from cricket_dataset.employees e1
join cricket_dataset.employees e2
on e1.managerId=e2.id
GROUP BY e1.managerId, e2.name
HAVING COUNT(e1.id) >= 5;

--total count of employees who don't have managers
select count(name) as names from cricket_dataset.employees
where managerid is null;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--find customers who have made purchases in all product categories.

DROP TABLE IF EXISTS cricket_dataset.customers;
-- Creating the Customers table
CREATE TABLE cricket_dataset.customers (
    customer_id INT64,
    customer_name string
);


DROP TABLE IF EXISTS cricket_dataset.purchases;
-- Creating the Purchases table
CREATE TABLE cricket_dataset.purchases (
    purchase_id INT64,
    customer_id INT64,
    product_category string,
);

-- Inserting sample data into Customers table
INSERT INTO cricket_dataset.customers (customer_id, customer_name) VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie'),
    (4, 'David'),
    (5, 'Emma');

-- Inserting sample data into Purchases table
INSERT INTO cricket_dataset.purchases (purchase_id, customer_id, product_category) VALUES
    (101, 1, 'Electronics'),
    (102, 1, 'Books'),
    (103, 1, 'Clothing'),
    (104, 1, 'Electronics'),
    (105, 2, 'Clothing'),
    (106, 1, 'Beauty'),
    (107, 3, 'Electronics'),
    (108, 3, 'Books'),
    (109, 4, 'Books'),
    (110, 4, 'Clothing'),
    (111, 4, 'Beauty'),
    (112, 5, 'Electronics'),
    (113, 5, 'Books');

-- cx_id, cx_name
-- find total distinct category 
-- how many distinct category each cx purchase from 
-- join both
--having clause will give us customer who have bought all 4 categories products

select c.customer_id,c.customer_name,count(distinct p.product_category) as total_cnt,
from cricket_dataset.customers c
join `cricket_dataset.purchases` p
on c.customer_id=p.customer_id
group by 1,2
having count(distinct p.product_category)=(select count(distinct product_category) from `cricket_dataset.purchases`)


--find customers who have not made any purchase in electronics category
select c.customer_id,c.customer_name,p.product_category,count(c.customer_id) as total_cnt from `cricket_dataset.customers` c
join `cricket_dataset.purchases` p
on c.customer_id = p.customer_id
where c.customer_id not in (SELECT distinct customer_id FROM `cricket_dataset.purchases` WHERE product_category like '%Electronics%')
group by c.customer_name,c.customer_id,p.product_category
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Write a SQL query to find out each hotal best performing months based on revenue 

CREATE TABLE hotel_bookings (
    booking_id int64,
    booking_date DATE,
    hotel_name string,
    total_guests INT64,
    total_nights INT64,
    total_price float64
);

-- Inserting sample data for hotel bookings for 2023 and 2022
INSERT INTO hotel_bookings (booking_id,booking_date, hotel_name, total_guests, total_nights, total_price) VALUES
    (1,'2023-01-05', 'Hotel A', 2, 3, 300.00),
    (2,'2023-02-10', 'Hotel B', 3, 5, 600.00),
    (3,'2023-03-15', 'Hotel A', 4, 2, 400.00),
    (4,'2023-04-20', 'Hotel B', 2, 4, 500.00),
    (5,'2023-05-25', 'Hotel A', 3, 3, 450.00),
    (6,'2023-06-30', 'Hotel B', 5, 2, 350.00),
    (7,'2023-07-05', 'Hotel A', 2, 5, 550.00),
    (8,'2023-08-10', 'Hotel B', 3, 3, 450.00),
    (9,'2023-09-15', 'Hotel A', 4, 4, 500.00),
    (10,'2023-10-20', 'Hotel B', 2, 3, 300.00),
    (11,'2023-11-25', 'Hotel A', 3, 2, 350.00),
    (12,'2023-12-30', 'Hotel B', 5, 4, 600.00),
    (13,'2022-01-05', 'Hotel A', 2, 3, 300.00),
    (14,'2022-02-10', 'Hotel B', 3, 5, 600.00),
    (15,'2022-03-15', 'Hotel A', 4, 2, 400.00),
    (16,'2022-04-20', 'Hotel B', 2, 4, 500.00),
    (17,'2022-05-25', 'Hotel A', 3, 3, 450.00),
    (18,'2022-06-30', 'Hotel B', 5, 2, 350.00),
    (19,'2022-07-05', 'Hotel A', 2, 5, 550.00),
    (20,'2022-08-10', 'Hotel B', 3, 3, 450.00),
    (21,'2022-09-15', 'Hotel A', 4, 4, 500.00),
    (22,'2022-10-20', 'Hotel B', 2, 3, 300.00),
    (23,'2022-11-25', 'Hotel A', 3, 2, 350.00),
    (24,'2022-12-30', 'Hotel B', 5, 4, 600.00);

select * from cricket_dataset.hotel_bookings;


--first take out month and year 
--then take total sales of hotel
--order by in desc
with cte1 as(
select extract(Month from booking_date) as month,extract(Year from booking_date) as year,hotel_name,sum(total_price) as total_rev
from cricket_dataset.hotel_bookings
group by 1,2,3
order by total_rev desc,year asc),
cte2 as(
    select year,month,hotel_name,total_rev,
    rank() over(partition by year,hotel_name order by total_rev desc) as rn
    from cte1
)
select year ,month ,hotel_name,total_rev  from cte2 where rn=1 order by month asc ;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Find the details of employees whose salary is greater than the average salary across the entire company.

DROP TABLE IF EXISTS cricket_dataset.employees;
-- Creating the employees table
CREATE TABLE cricket_dataset.employees (
    employee_id int64,
    employee_name string,
    department string,
    salary float64
);

INSERT INTO cricket_dataset.employees (employee_id,employee_name, department, salary) 
VALUES
    (1,'John Doe', 'HR', 50000.00),
    (2,'Jane Smith', 'HR', 55000.00),
    (3,'Michael Johnson', 'HR', 60000.00),
    (4,'Emily Davis', 'IT', 60000.00),
    (5,'David Brown', 'IT', 65000.00),
    (6,'Sarah Wilson', 'Finance', 70000.00),
    (7,'Robert Taylor', 'Finance', 75000.00),
    (8,'Jennifer Martinez', 'Finance', 80000.00);

select * from cricket_dataset.employees;

--use avg() function
select * from `cricket_dataset.employees` where salary > (select avg(salary) from `cricket_dataset.employees`)

--Find the average salary of employees in each department, along with the total number of employees in that department.
select department,avg(salary) as avg_salary, count(employee_name) as no_of_employees
from `cricket_dataset.employees`
group by department
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
--query to find products that are sold by both Supplier A and Supplier B, excluding products sold by only one supplier.

DROP TABLE IF EXISTS cricket_dataset.products;
CREATE TABLE cricket_dataset.products (
    product_id INT64,
    product_name string,
    supplier_name string
);

INSERT INTO cricket_dataset.products (product_id, product_name, supplier_name) VALUES
    (1, 'Product 1', 'Supplier A'),
    (1, 'Product 1', 'Supplier B'),
    (3, 'Product 3', 'Supplier A'),
    (3, 'Product 3', 'Supplier A'),
    (5, 'Product 5', 'Supplier A'),
    (5, 'Product 5', 'Supplier B'),
    (7, 'Product 7', 'Supplier C'),
    (8, 'Product 8', 'Supplier A'),
    (7, 'Product 7', 'Supplier B'),
    (7, 'Product 7', 'Supplier A'),
    (9, 'Product 9', 'Supplier B'),
    (9, 'Product 9', 'Supplier C'),
    (10, 'Product 10', 'Supplier C'),
    (11, 'Product 11', 'Supplier C'),
    (10, 'Product 10', 'Supplier A');

select * from `cricket_dataset.products`

--find the product and supplier 
--then take count of suppliers group by id and name and where count=2

select product_id,product_name,count(supplier_name) as total_suppliers  from cricket_dataset.products
where supplier_name in ('Supplier A','Supplier B')
group by product_id,product_name
having count(distinct supplier_name)=2

--Find the product that are selling by Supplier C and Supplier B but not Supplier A
select product_id,product_name,count(*) from cricket_dataset.products
where supplier_name in ('Supplier C','Supplier B')
group by product_id,product_name
having count(distinct supplier_name) =2
------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Calculate the percentage contribution of each product to total revenue?
DROP TABLE IF EXISTS cricket_dataset.products;
-- Creating the products table
CREATE TABLE cricket_dataset.products (
    product_id INT64,
    product_name string,
    price float64,
    quantity_sold INT64
);

-- Inserting sample data for products
INSERT INTO cricket_dataset.products (product_id, product_name, price, quantity_sold) VALUES
    (1, 'iPhone', 899.00, 600),
    (2, 'iMac', 1299.00, 150),
    (3, 'MacBook Pro', 1499.00, 500),
    (4, 'AirPods', 499.00, 800),
    (5, 'Accessories', 199.00, 300);

--find total sale of each products , total revenue also
select product_id,product_name,price*quantity_sold as sale_by_prod,
round((price*quantity_sold/(select sum(price*quantity_sold) from cricket_dataset.products)),2)*100 as total_percentage
from cricket_dataset.products


--Find what is the contribution of MacBook Pro and iPhone Round the result in two DECIMAL
select product_id,product_name,price*quantity_sold as sale_by_prod,
round((price*quantity_sold/(select sum(price*quantity_sold) from cricket_dataset.products)),2)*100 as total_percentage
from cricket_dataset.products
where product_name in ('MacBook Pro','iPhone')
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*Question

You have dataset of a food delivery company
with columns order_id, customer_id, order_date, 
pref_delivery_date

If the customer's preferred delivery date is 
the same as the order date, then the order is 
called immediate; otherwise, it is called scheduled.

Write a solution to find the percentage of immediate
orders in the first orders of all customers, 
rounded to 2 decimal places.*/

DROP TABLE IF EXISTS cricket_dataset.delivery;
-- Create the Delivery table
CREATE TABLE cricket_dataset.delivery (
    delivery_id int64,
    customer_id INT64,
    order_date DATE,
    customer_pref_delivery_date DATE
);

-- Insert data into the Delivery table
INSERT INTO cricket_dataset.delivery (delivery_id,customer_id, order_date, customer_pref_delivery_date) VALUES
(1,1, '2019-08-01', '2019-08-02'),
(2,2, '2019-08-02', '2019-08-02'),
(3,1, '2019-08-11', '2019-08-12'),
(4,3, '2019-08-24', '2019-08-24'),
(5,3, '2019-08-21', '2019-08-22'),
(6,2, '2019-08-11', '2019-08-13'),
(7,4, '2019-08-09', '2019-08-09'),
(8,5, '2019-08-09', '2019-08-10'),
(9,4, '2019-08-10', '2019-08-12'),
(10,6, '2019-08-09', '2019-08-11'),
(11,7, '2019-08-12', '2019-08-13'),
(12,8, '2019-08-13', '2019-08-13'),
(13,9, '2019-08-11', '2019-08-12');

--find first order for each cx
--total count of first orders
--set case as immediate or scheduled
--total immediate orders/cnt of first orders *100 for percentage

select *, 
round(sum(case when order_date = first_orders.cpdd then 1 else 2 end as dd)/count(*)*100,2)
from(
select distinct customer_id,order_date,customer_pref_delivery_date as cpdd 
from cricket_dataset.delivery
order by customer_id,order_date) as first_orders

/*
Write an SQL query to determine the percentage of orders where customers select next day delivery. We're excited to see your solution! 
-- Next Day Delivery is Order Date + 1
*/
select 
	round(sum (case when next_day_od = cpdd then 1
	else 0
	end::numeric )/count(*)::numeric * 100 , 2) as nextday_del_percentage
from
    (select 
        distinct on (customer_id)
        customer_id,
        order_date,
        customer_pref_delivery_date as cpdd,
        order_date + 1 as next_day_od
        from cricket_dataset.delivery
        order by customer_id,order_date
    ) x
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Write a query that'll identify returning active users. 

A returning active user is a user that has made a 
second purchase within 7 days of their first purchase

Output a list of user_ids of these returning active users. */

DROP TABLE IF EXISTS cricket_dataset.amazon_transactions;
CREATE TABLE cricket_dataset.amazon_transactions (
    id int64,
    user_id INT64,
    item string,
    purchase_date DATE,
    revenue int64
);

INSERT INTO cricket_dataset.amazon_transactions (id,user_id, item, purchase_date, revenue) VALUES
(1,109, 'milk', '2020-03-03', 123),
(2,139, 'biscuit', '2020-03-18', 421),
(3,120, 'milk', '2020-03-18', 176),
(4,108, 'banana', '2020-03-18', 862),
(5,130, 'milk', '2020-03-28', 333),
(6,103, 'bread', '2020-03-29', 862),
(7,122, 'banana', '2020-03-07', 952),
(8,125, 'bread', '2020-03-13', 317),
(9,139, 'bread', '2020-03-30', 929),
(10,141, 'banana', '2020-03-17', 812),
(11,116, 'bread', '2020-03-31', 226),
(12,128, 'bread', '2020-03-04', 112),
(13,146, 'biscuit', '2020-03-04', 362),
(14,119, 'banana', '2020-03-28', 127),
(15,142, 'bread', '2020-03-09', 503),
(16,122, 'bread', '2020-03-06', 593),
(17,128, 'biscuit', '2020-03-24', 160),
(18,112, 'banana', '2020-03-24', 262),
(19,149, 'banana', '2020-03-29', 382),
(20,100, 'banana', '2020-03-18', 599),
(21,130, 'milk', '2020-03-16', 604),
(22,103, 'milk', '2020-03-31', 290),
(23,112, 'banana', '2020-03-23', 523),
(24,102, 'bread', '2020-03-25', 325),
(25,120, 'biscuit', '2020-03-21', 858),
(26,109, 'bread', '2020-03-22', 432),
(27,101, 'milk', '2020-03-01', 449),
(28,138, 'milk', '2020-03-19', 961),
(29,100, 'milk', '2020-03-29', 410),
(30,129, 'milk', '2020-03-02', 771),
(31,123, 'milk', '2020-03-31', 434),
(32,104, 'biscuit', '2020-03-31', 957),
(33,110, 'bread', '2020-03-13', 210),
(34,143, 'bread', '2020-03-27', 870),
(35,130, 'milk', '2020-03-12', 176),
(36,128, 'milk', '2020-03-28', 498),
(37,133, 'banana', '2020-03-21', 837),
(38,150, 'banana', '2020-03-20', 927),
(39,120, 'milk', '2020-03-27', 793),
(40,109, 'bread', '2020-03-02', 362),
(41,110, 'bread', '2020-03-13', 262),
(42,140, 'milk', '2020-03-09', 468),
(43,112, 'banana', '2020-03-04', 381),
(44,117, 'biscuit', '2020-03-19', 831),
(45,137, 'banana', '2020-03-23', 490),
(46,130, 'bread', '2020-03-09', 149),
(47,133, 'bread', '2020-03-08', 658),
(48,143, 'milk', '2020-03-11', 317),
(49,111, 'biscuit', '2020-03-23', 204),
(50,150, 'banana', '2020-03-04', 299),
(51,131, 'bread', '2020-03-10', 155),
(52,140, 'biscuit', '2020-03-17', 810),
(53,147, 'banana', '2020-03-22', 702),
(54,119, 'biscuit', '2020-03-15', 355),
(55,116, 'milk', '2020-03-12', 468),
(56,141, 'milk', '2020-03-14', 254),
(57,143, 'bread', '2020-03-16', 647),
(58,105, 'bread', '2020-03-21', 562),
(59,149, 'biscuit', '2020-03-11', 827),
(60,117, 'banana', '2020-03-22', 249),
(61,150, 'banana', '2020-03-21', 450),
(62,134, 'bread', '2020-03-08', 981),
(63,133, 'banana', '2020-03-26', 353),
(64,127, 'milk', '2020-03-27', 300),
(65,101, 'milk', '2020-03-26', 740),
(66,137, 'biscuit', '2020-03-12', 473),
(67,113, 'biscuit', '2020-03-21', 278),
(68,141, 'bread', '2020-03-21', 118),
(69,112, 'biscuit', '2020-03-14', 334),
(70,118, 'milk', '2020-03-30', 603),
(71,111, 'milk', '2020-03-19', 205),
(72,146, 'biscuit', '2020-03-13', 599),
(73,148, 'banana', '2020-03-14', 530),
(74,100, 'banana', '2020-03-13', 175),
(75,105, 'banana', '2020-03-05', 815),
(76,129, 'milk', '2020-03-02', 489),
(77,121, 'milk', '2020-03-16', 476),
(78,117, 'bread', '2020-03-11', 270),
(79,133, 'milk', '2020-03-12', 446),
(80,124, 'bread', '2020-03-31', 937),
(81,145, 'bread', '2020-03-07', 821),
(82,105, 'banana', '2020-03-09', 972),
(83,131, 'milk', '2020-03-09', 808),
(84,114, 'biscuit', '2020-03-31', 202),
(85,120, 'milk', '2020-03-06', 898),
(86,130, 'milk', '2020-03-06', 581),
(87,141, 'biscuit', '2020-03-11', 749),
(88,147, 'bread', '2020-03-14', 262),
(89,118, 'milk', '2020-03-15', 735),
(90,136, 'biscuit', '2020-03-22', 410),
(91,132, 'bread', '2020-03-06', 161),
(92,137, 'biscuit', '2020-03-31', 427),
(93,107, 'bread', '2020-03-01', 701),
(94,111, 'biscuit', '2020-03-18', 218),
(95,100, 'bread', '2020-03-07', 410),
(96,106, 'milk', '2020-03-21', 379),
(97,114, 'banana', '2020-03-25', 705),
(98,110, 'bread', '2020-03-27', 225),
(99,130, 'milk', '2020-03-16', 494),
(100,117, 'bread', '2020-03-10', 209);


select * from `cricket_dataset.amazon_transactions`;

--users first purchase then second purchase within >=7 days
--join and then select distinct users

select a1.user_id,a1.purchase_date as first_purchase_date,
a2.purchase_date as second_purchase_date
from cricket_dataset.amazon_transactions a1
join cricket_dataset.amazon_transactions a2
on a1.user_id=a2.user_id
and a1.purchase_date < a2.purchase_date
and date_diff(a2.purchase_date,a1.purchase_date,DAY) <=7
order by 1

--Find the user_id who has not purchased anything for 7 days after first purchase but they have done second purchase after 7 days 
select distinct a1.user_id,a1.purchase_date as first_purchase_date,
a2.purchase_date as second_purchase_date,date_diff(a2.purchase_date,a1.purchase_date,DAY) as diff
from cricket_dataset.amazon_transactions a1
join cricket_dataset.amazon_transactions a2
on a1.user_id=a2.user_id
and a1.purchase_date < a2.purchase_date
and date_diff(a2.purchase_date,a1.purchase_date,DAY)>7
order by 1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*Calculate the total revenue from each customer in March 2019. 
Include only customers who were active in March 2019.
Output the revenue along with the customer id and sort the results based on the revenue in descending order.*/
DROP TABLE IF EXISTS cricket_dataset.orders;

CREATE TABLE cricket_dataset.orders (
    id INT64,
    cust_id INT64,
    order_date DATE,
    order_details string,
    total_order_cost INT64
);

INSERT INTO cricket_dataset.orders (id, cust_id, order_date, order_details, total_order_cost) VALUES
(1, 7, '2019-03-04', 'Coat', 100),
(2, 7, '2019-03-01', 'Shoes', 80),
(3, 3, '2019-03-07', 'Skirt', 30),
(4, 7, '2019-02-01', 'Coat', 25),
(5, 7, '2019-03-10', 'Shoes', 80),
(6, 1, '2019-02-01', 'Boats', 100),
(7, 2, '2019-01-11', 'Shirts', 60),
(8, 1, '2019-03-11', 'Slipper', 20),
(9, 15, '2019-03-01', 'Jeans', 80),
(10, 15, '2019-03-09', 'Shirts', 50),
(11, 5, '2019-02-01', 'Shoes', 80),
(12, 12, '2019-01-11', 'Shirts', 60),
(13, 1, '2019-03-11', 'Slipper', 20),
(14, 4, '2019-02-01', 'Shoes', 80),
(15, 4, '2019-01-11', 'Shirts', 60),
(16, 3, '2019-04-19', 'Shirts', 50),
(17, 7, '2019-04-19', 'Suit', 150),
(18, 15, '2019-04-19', 'Skirt', 30),
(19, 15, '2019-04-20', 'Dresses', 200),
(20, 12, '2019-01-11', 'Coat', 125),
(21, 7, '2019-04-01', 'Suit', 50),
(22, 3, '2019-04-02', 'Skirt', 30),
(23, 4, '2019-04-03', 'Dresses', 50),
(24, 2, '2019-04-04', 'Coat', 25),
(25, 7, '2019-04-19', 'Coat', 125);

--find sum(total_order_cost) and customer_id
--filter on march 19

select cust_id,sum(total_order_cost) as total_revenue
from cricket_dataset.orders
where order_date between '2019-03-01' and '2019-03-30'
order by 2 desc

--Find the customers who purchased from both March and April of 2019 and their total revenue 
SELECT cust_id, SUM(total_order_cost) AS total_revenue
FROM cricket_dataset.orders
where EXTRACT(year  from  order_date) = 2019
and  EXTRACT( month from  order_date) in  (3, 4)
group by cust_id
having  count( distinct EXTRACT(MONTH FROM order_date)) = 2;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--You have given two tables customers with columns (id, name phone address) and orders table columns(order_id, cxid order_date and cost)
--Find the percentage of shipable orders.Consider an order is shipable if the customer's address is known.

DROP TABLE cricket_dataset.customers;
-- Creating the customers table
CREATE TABLE cricket_dataset.customers (
    id INT64,
    first_name string,
    last_name string,
    city string,
    address string,
    phone_number string
);

-- Inserting sample data into the customers table
INSERT INTO cricket_dataset.customers (id, first_name, last_name, city, address, phone_number) VALUES
    (8, 'John', 'Joseph', 'San Francisco', NULL, '928868164'),
    (7, 'Jill', 'Michael', 'Austin', NULL, '8130567692'),
    (4, 'William', 'Daniel', 'Denver', NULL, '813155200'),
    (5, 'Henry', 'Jackson', 'Miami', NULL, '8084557513'),
    (13, 'Emma', 'Isaac', 'Miami', NULL, '808690201'),
    (14, 'Liam', 'Samuel', 'Miami', NULL, '808555201'),
    (15, 'Mia', 'Owen', 'Miami', NULL, '806405201'),
    (1, 'Mark', 'Thomas', 'Arizona', '4476 Parkway Drive', '602325916'),
    (12, 'Eva', 'Lucas', 'Arizona', '4379 Skips Lane', '3019509805'),
    (6, 'Jack', 'Aiden', 'Arizona', '4833 Coplin Avenue', '480230527'),
    (2, 'Mona', 'Adrian', 'Los Angeles', '1958 Peck Court', '714939432'),
    (10, 'Lili', 'Oliver', 'Los Angeles', '3832 Euclid Avenue', '5306951180'),
    (3, 'Farida', 'Joseph', 'San Francisco', '3153 Rhapsody Street', '8133681200'),
    (9, 'Justin', 'Alexander', 'Denver', '4470 McKinley Avenue', '9704337589'),
    (11, 'Frank', 'Jacob', 'Miami', '1299 Randall Drive', '8085905201');

drop table if exists cricket_dataset.orders;
-- Creating the orders table
CREATE TABLE cricket_dataset.orders (
    id INT64,
    cust_id INT64,
    order_date DATE,
    order_details string,
    total_order_cost INT64
);

-- Inserting sample data into the orders table
INSERT INTO cricket_dataset.orders (id, cust_id, order_date, order_details, total_order_cost) VALUES
    (1, 3, '2019-03-04', 'Coat', 100),
    (2, 3, '2019-03-01', 'Shoes', 80),
    (3, 3, '2019-03-07', 'Skirt', 30),
    (4, 7, '2019-02-01', 'Coat', 25),
    (5, 7, '2019-03-10', 'Shoes', 80),
    (6, 15, '2019-02-01', 'Boats', 100),
    (7, 15, '2019-01-11', 'Shirts', 60),
    (8, 15, '2019-03-11', 'Slipper', 20),
    (9, 15, '2019-03-01', 'Jeans', 80),
    (10, 15, '2019-03-09', 'Shirts', 50),
    (11, 5, '2019-02-01', 'Shoes', 80),
    (12, 12, '2019-01-11', 'Shirts', 60),
    (13, 12, '2019-03-11', 'Slipper', 20),
    (14, 4, '2019-02-01', 'Shoes', 80),
    (15, 4, '2019-01-11', 'Shirts', 60),
    (16, 3, '2019-04-19', 'Shirts', 50),
    (17, 7, '2019-04-19', 'Suit', 150),
    (18, 15, '2019-04-19', 'Skirt', 30),
    (19, 15, '2019-04-20', 'Dresses', 200),
    (20, 12, '2019-01-11', 'Coat', 125),
    (21, 7, '2019-04-01', 'Suit', 50),
    (22, 7, '2019-04-02', 'Skirt', 30),
    (23, 7, '2019-04-03', 'Dresses', 50),
    (24, 7, '2019-04-04', 'Coat', 25),
    (25, 7, '2019-04-19', 'Coat', 125);

--first find percentage as shipable_order/total_orders*100
-- find total orders
-- total shipable orders where address is not NULL
-- shipable orders/total orders * 100
select
round((sum(case when c.address is not null then 1 else 0 end)/count(*))*100,2) as shippable_orders
from `cricket_dataset.orders` o
join cricket_dataset.customers c
on o.cust_id=c.id

--find out percentage of customers who don't have valid phone numbers. Valid phone numbers is of 10characters
select 
    round( sum(case
    when length(c.phone_number) <> 10 then 1
    else 0
    end)/count(*)*100,2) as per_cust_novalidph
from cricket_dataset.orders o
left join cricket_dataset.customers c
on o.cust_id = c.id;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    department VARCHAR(100),
    salary DECIMAL(10, 2),
    manager_id INT
);

INSERT INTO employees (employee_id, employee_name, department, salary, manager_id)
VALUES
    (1, 'John Doe', 'HR', 50000.00, NULL),
    (2, 'Jane Smith', 'HR', 55000.00, 1),
    (3, 'Michael Johnson', 'HR', 60000.00, 1),
    (4, 'Emily Davis', 'IT', 60000.00, NULL),
    (5, 'David Brown', 'IT', 65000.00, 4),
    (6, 'Sarah Wilson', 'Finance', 70000.00, NULL),
    (7, 'Robert Taylor', 'Finance', 75000.00, 6),
    (8, 'Jennifer Martinez', 'Finance', 80000.00, 6);



/*
-- Question
You have a employees table with columns emp_id, emp_name,
department, salary, manager_id (manager is also emp in the table))

Identify employees who have a higher salary than their manager. 
*/



SELECT 
    e.employee_id,
    e.employee_name,
    e.department,
    e.salary,
    e.manager_id,
    m.employee_name as manager_name,
    m.salary as manager_salary
from employees as e
JOIN
employees as m
ON e.manager_id = m.employee_id
WHERE e.salary > m.salary

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Find the best selling item for each month (no need to separate months by year) where the biggest total invoice was paid. 

The best selling item is calculated using the formula 
(unitprice * quantity). Output the month, the description of the item along with the amount paid.
*/
DROP TABLE IF EXISTS cricket_dataset.walmart_eu;
-- Create the online_retail table
CREATE TABLE cricket_dataset.walmart_eu (
    invoiceno string,
    stockcode string,
    description string,
    quantity INT64,
    invoicedate DATE,
    unitprice FLOAT64,
    customerid FLOAT64,
    country string
);

-- Insert the provided data into the online_retail table
INSERT INTO cricket_dataset.walmart_eu (invoiceno, stockcode, description, quantity, invoicedate, unitprice, customerid, country) VALUES
('544586', '21890', 'S/6 WOODEN SKITTLES IN COTTON BAG', 3, '2011-02-21', 2.95, 17338, 'United Kingdom'),
('541104', '84509G', 'SET OF 4 FAIRY CAKE PLACEMATS', 3, '2011-01-13', 3.29, NULL, 'United Kingdom'),
('560772', '22499', 'WOODEN UNION JACK BUNTING', 3, '2011-07-20', 4.96, NULL, 'United Kingdom'),
('555150', '22488', 'NATURAL SLATE RECTANGLE CHALKBOARD', 5, '2011-05-31', 3.29, NULL, 'United Kingdom'),
('570521', '21625', 'VINTAGE UNION JACK APRON', 3, '2011-10-11', 6.95, 12371, 'Switzerland'),
('547053', '22087', 'PAPER BUNTING WHITE LACE', 40, '2011-03-20', 2.55, 13001, 'United Kingdom'),
('573360', '22591', 'CARDHOLDER GINGHAM CHRISTMAS TREE', 6, '2011-10-30', 3.25, 15748, 'United Kingdom'),
('571039', '84536A', 'ENGLISH ROSE NOTEBOOK A7 SIZE', 1, '2011-10-13', 0.42, 16121, 'United Kingdom'),
('578936', '20723', 'STRAWBERRY CHARLOTTE BAG', 10, '2011-11-27', 0.85, 16923, 'United Kingdom'),
('559338', '21391', 'FRENCH LAVENDER SCENT HEART', 1, '2011-07-07', 1.63, NULL, 'United Kingdom'),
('568134', '23171', 'REGENCY TEA PLATE GREEN', 1, '2011-09-23', 3.29, NULL, 'United Kingdom'),
('552061', '21876', 'POTTERING MUG', 12, '2011-05-06', 1.25, 13001, 'United Kingdom'),
('543179', '22531', 'MAGIC DRAWING SLATE CIRCUS PARADE', 1, '2011-02-04', 0.42, 12754, 'Japan'),
('540954', '22381', 'TOY TIDY PINK POLKADOT', 4, '2011-01-12', 2.1, 14606, 'United Kingdom'),
('572703', '21818', 'GLITTER HEART DECORATION', 13, '2011-10-25', 0.39, 16110, 'United Kingdom'),
('578757', '23009', 'I LOVE LONDON BABY GIFT SET', 1, '2011-11-25', 16.95, 12748, 'United Kingdom'),
('542616', '22505', 'MEMO BOARD COTTAGE DESIGN', 4, '2011-01-30', 4.95, 16816, 'United Kingdom'),
('554694', '22921', 'HERB MARKER CHIVES', 1, '2011-05-25', 1.63, NULL, 'United Kingdom'),
('569545', '21906', 'PHARMACIE FIRST AID TIN', 1, '2011-10-04', 13.29, NULL, 'United Kingdom'),
('549562', '21169', 'YOU ARE CONFUSING ME METAL SIGN', 1, '2011-04-10', 1.69, 13232, 'United Kingdom'),
('580610', '21945', 'STRAWBERRIES DESIGN FLANNEL', 1, '2011-12-05', 1.63, NULL, 'United Kingdom'),
('558066', 'gift_0001_50', 'Dotcomgiftshop Gift Voucher £50.00', 1, '2011-06-24', 41.67, NULL, 'United Kingdom'),
('538349', '21985', 'PACK OF 12 HEARTS DESIGN TISSUES', 1, '2010-12-10', 0.85, NULL, 'United Kingdom'),
('537685', '22737', 'RIBBON REEL CHRISTMAS PRESENT', 15, '2010-12-08', 1.65, 18077, 'United Kingdom'),
('545906', '22614', 'PACK OF 12 SPACEBOY TISSUES', 24, '2011-03-08', 0.29, 15764, 'United Kingdom'),
('550997', '22629', 'SPACEBOY LUNCH BOX', 12, '2011-04-26', 1.95, 17735, 'United Kingdom'),
('558763', '22960', 'JAM MAKING SET WITH JARS', 3, '2011-07-03', 4.25, 12841, 'United Kingdom'),
('562688', '22918', 'HERB MARKER PARSLEY', 12, '2011-08-08', 0.65, 13869, 'United Kingdom'),
('541424', '84520B', 'PACK 20 ENGLISH ROSE PAPER NAPKINS', 9, '2011-01-17', 1.63, NULL, 'United Kingdom'),
('581405', '20996', 'JAZZ HEARTS ADDRESS BOOK', 1, '2011-12-08', 0.19, 13521, 'United Kingdom'),
('571053', '23256', 'CHILDRENS CUTLERY SPACEBOY', 4, '2011-10-13', 4.15, 12631, 'Finland'),
('563333', '23012', 'GLASS APOTHECARY BOTTLE PERFUME', 1, '2011-08-15', 3.95, 15996, 'United Kingdom'),
('568054', '47559B', 'TEA TIME OVEN GLOVE', 4, '2011-09-23', 1.25, 16978, 'United Kingdom'),
('574262', '22561', 'WOODEN SCHOOL COLOURING SET', 12, '2011-11-03', 1.65, 13721, 'United Kingdom'),
('569360', '23198', 'PANTRY MAGNETIC SHOPPING LIST', 6, '2011-10-03', 1.45, 14653, 'United Kingdom'),
('570210', '22980', 'PANTRY SCRUBBING BRUSH', 2, '2011-10-09', 1.65, 13259, 'United Kingdom'),
('576599', '22847', 'BREAD BIN DINER STYLE IVORY', 1, '2011-11-15', 16.95, 14544, 'United Kingdom'),
('579777', '22356', 'CHARLOTTE BAG PINK POLKADOT', 4, '2011-11-30', 1.63, NULL, 'United Kingdom'),
('566060', '21106', 'CREAM SLICE FLANNEL CHOCOLATE SPOT', 1, '2011-09-08', 5.79, NULL, 'United Kingdom'),
('550514', '22489', 'PACK OF 12 TRADITIONAL CRAYONS', 24, '2011-04-18', 0.42, 14631, 'United Kingdom'),
('569898', '23437', '50S CHRISTMAS GIFT BAG LARGE', 2, '2011-10-06', 2.46, NULL, 'United Kingdom'),
('563566', '23548', 'WRAP MAGIC FOREST', 25, '2011-08-17', 0.42, 13655, 'United Kingdom'),
('559693', '21169', 'YOURE CONFUSING ME METAL SIGN', 1, '2011-07-11', 4.13, NULL, 'United Kingdom'),
('573386', '22112', 'CHOCOLATE HOT WATER BOTTLE', 24, '2011-10-30', 4.25, 17183, 'United Kingdom'),
('576920', '23312', 'VINTAGE CHRISTMAS GIFT SACK', 4, '2011-11-17', 4.15, 13871, 'United Kingdom'),
('564473', '22384', 'LUNCH BAG PINK POLKADOT', 10, '2011-08-25', 1.65, 16722, 'United Kingdom'),
('562264', '23321', 'SMALL WHITE HEART OF WICKER', 3, '2011-08-03', 3.29, NULL, 'United Kingdom'),
('542541', '79030D', 'TUMBLER, BAROQUE', 1, '2011-01-28', 12.46, NULL, 'United Kingdom'),
('579937', '22090', 'PAPER BUNTING RETROSPOT', 12, '2011-12-01', 2.95, 13509, 'United Kingdom'),
('574076', '22483', 'RED GINGHAM TEDDY BEAR', 1, '2011-11-02', 5.79, NULL, 'United Kingdom'),
('579187', '20665', 'RED RETROSPOT PURSE', 1, '2011-11-28', 5.79, NULL, 'United Kingdom'),
('542922', '22423', 'REGENCY CAKESTAND 3 TIER', 3, '2011-02-02', 12.75, 12682, 'France'),
('570677', '23008', 'DOLLY GIRL BABY GIFT SET', 2, '2011-10-11', 16.95, 12836, 'United Kingdom'),
('577182', '21930', 'JUMBO STORAGE BAG SKULLS', 10, '2011-11-18', 2.08, 16945, 'United Kingdom'),
('576686', '20992', 'JAZZ HEARTS PURSE NOTEBOOK', 1, '2011-11-16', 0.39, 16916, 'United Kingdom'),
('553844', '22569', 'FELTCRAFT CUSHION BUTTERFLY', 4, '2011-05-19', 3.75, 13450, 'United Kingdom'),
('580689', '23150', 'IVORY SWEETHEART SOAP DISH', 6, '2011-12-05', 2.49, 12994, 'United Kingdom'),
('545000', '85206A', 'CREAM FELT EASTER EGG BASKET', 6, '2011-02-25', 1.65, 15281, 'United Kingdom'),
('541975', '22382', 'LUNCH BAG SPACEBOY DESIGN', 40, '2011-01-24', 1.65, NULL, 'Hong Kong'),
('544942', '22551', 'PLASTERS IN TIN SPACEBOY', 12, '2011-02-25', 1.65, 15544, 'United Kingdom'),
('543177', '22667', 'RECIPE BOX RETROSPOT', 6, '2011-02-04', 2.95, 14466, 'United Kingdom'),
('574587', '23356', 'LOVE HOT WATER BOTTLE', 4, '2011-11-06', 5.95, 14936, 'Channel Islands'),
('543451', '22774', 'RED DRAWER KNOB ACRYLIC EDWARDIAN', 1, '2011-02-08', 2.46, NULL, 'United Kingdom'),
('578270', '22579', 'WOODEN TREE CHRISTMAS SCANDINAVIAN', 1, '2011-11-23', 1.63, 14096, 'United Kingdom'),
('551413', '84970L', 'SINGLE HEART ZINC T-LIGHT HOLDER', 12, '2011-04-28', 0.95, 16227, 'United Kingdom'),
('567666', '22900', 'SET 2 TEA TOWELS I LOVE LONDON', 6, '2011-09-21', 3.25, 12520, 'Germany'),
('571544', '22810', 'SET OF 6 T-LIGHTS SNOWMEN', 2, '2011-10-17', 2.95, 17757, 'United Kingdom'),
('558368', '23249', 'VINTAGE RED ENAMEL TRIM PLATE', 12, '2011-06-28', 1.65, 14329, 'United Kingdom'),
('546430', '22284', 'HEN HOUSE DECORATION', 2, '2011-03-13', 1.65, 15918, 'United Kingdom'),
('565233', '23000', 'TRAVEL CARD WALLET TRANSPORT', 1, '2011-09-02', 0.83, NULL, 'United Kingdom'),
('559984', '16012', 'FOOD/DRINK SPONGE STICKERS', 50, '2011-07-14', 0.21, 16657, 'United Kingdom'),
('576920', '23312', 'VINTAGE CHRISTMAS GIFT SACK', -4, '2011-11-17', 4.15, 13871, 'United Kingdom'),
('564473', '22384', 'LUNCH BAG PINK POLKADOT', 10, '2011-08-25', 1.65, 16722, 'United Kingdom'),
('562264', '23321', 'SMALL WHITE HEART OF WICKER', 3, '2011-08-03', 3.29, NULL, 'United Kingdom'),
('542541', '79030D', 'TUMBLER, BAROQUE', 1, '2011-01-28', 12.46, NULL, 'United Kingdom'),
('579937', '22090', 'PAPER BUNTING RETROSPOT', 12, '2011-12-01', 2.95, 13509, 'United Kingdom'),
('574076', '22483', 'RED GINGHAM TEDDY BEAR', 1, '2011-11-02', 5.79, NULL, 'United Kingdom'),
('579187', '20665', 'RED RETROSPOT PURSE', 1, '2011-11-28', 5.79, NULL, 'United Kingdom'),
('542922', '22423', 'REGENCY CAKESTAND 3 TIER', 3, '2011-02-02', 12.75, 12682, 'France'),
('570677', '23008', 'DOLLY GIRL BABY GIFT SET', 2, '2011-10-11', 16.95, 12836, 'United Kingdom'),
('577182', '21930', 'JUMBO STORAGE BAG SKULLS', 10, '2011-11-18', 2.08, 16945, 'United Kingdom'),
('576686', '20992', 'JAZZ HEARTS PURSE NOTEBOOK', 1, '2011-11-16', 0.39, 16916, 'United Kingdom'),
('553844', '22569', 'FELTCRAFT CUSHION BUTTERFLY', 4, '2011-05-19', 3.75, 13450, 'United Kingdom'),
('580689', '23150', 'IVORY SWEETHEART SOAP DISH', 6, '2011-12-05', 2.49, 12994, 'United Kingdom'),
('545000', '85206A', 'CREAM FELT EASTER EGG BASKET', 6, '2011-02-25', 1.65, 15281, 'United Kingdom'),
('541975', '22382', 'LUNCH BAG SPACEBOY DESIGN', 40, '2011-01-24', 1.65, NULL, 'Hong Kong'),
('544942', '22551', 'PLASTERS IN TIN SPACEBOY', 12, '2011-02-25', 1.65, 15544, 'United Kingdom'),
('543177', '22667', 'RECIPE BOX RETROSPOT', 6, '2011-02-04', 2.95, 14466, 'United Kingdom'),
('574587', '23356', 'LOVE HOT WATER BOTTLE', 4, '2011-11-06', 5.95, 14936, 'Channel Islands'),
('543451', '22774', 'RED DRAWER KNOB ACRYLIC EDWARDIAN', 1, '2011-02-08', 2.46, NULL, 'United Kingdom'),
('578270', '22579', 'WOODEN TREE CHRISTMAS SCANDINAVIAN', 1, '2011-11-23', 1.63, 14096, 'United Kingdom'),
('551413', '84970L', 'SINGLE HEART ZINC T-LIGHT HOLDER', 12, '2011-04-28', 0.95, 16227, 'United Kingdom'),
('567666', '22900', 'SET 2 TEA TOWELS I LOVE LONDON', 6, '2011-09-21', 3.25, 12520, 'Germany'),
('571544', '22810', 'SET OF 6 T-LIGHTS SNOWMEN', 2, '2011-10-17', 2.95, 17757, 'United Kingdom'),
('558368', '23249', 'VINTAGE RED ENAMEL TRIM PLATE', 12, '2011-06-28', 1.65, 14329, 'United Kingdom'),
('546430', '22284', 'HEN HOUSE DECORATION', 2, '2011-03-13', 1.65, 15918, 'United Kingdom'),
('565233', '23000', 'TRAVEL CARD WALLET TRANSPORT', 1, '2011-09-02', 0.83, NULL, 'United Kingdom'),
('559984', '16012', 'FOOD/DRINK SPONGE STICKERS', 50, '2011-07-14', 0.21, 16657, 'United Kingdom');

-- month invoice data
-- group by product desc
-- revenue price * qty
-- rank 
-- subquery 

SELECT
    month,
    description,
    total_sale
FROM
(
  SELECT EXTRACT(MONTH FROM invoicedate) as month,description,SUM(unitprice * quantity) as total_sale,
  RANK() OVER(PARTITION BY EXTRACT(MONTH FROM invoicedate) ORDER BY SUM(unitprice * quantity) DESC) as rn
  FROM cricket_dataset.walmart_eu
  GROUP BY month, description
) as subquery
WHERE rn= 1

--Find Customer of the month from each MONTH one customer who has spent the highest amount (price * quantity) as total amount may include multiple purchase
select customerid , month , description , total_invoice from
(select distinct(customerid) , month(invoicedate), monthname(invoicedate) as month_ ,  
 description , round(sum(unitprice * quantity )) as total_invoice,
 rank() over(partition by month(invoicedate) order by round(sum(unitprice * quantity )) desc ) rk
from cricket_dataset.walmart_eu 
where customerid is not null
group by 1 ,2,3,4
order by 2 , 5 desc) sq
where rk = 1;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
--Question
Write a query to find the highest-selling 
product for each customer

Return cx id, product description, 
and total count of purchase.

*/ 
-- cx all product they purchased and their total orders
-- order by by number of purchase desc
-- 1 product that has highest purchase 
-- rank 

SELECT * FROM cricket_dataset.walmart_eu;

SELECT *
FROM
(
  SELECT customerid,description,COUNT(*) as total_purchase,
  RANK() OVER(PARTITION BY cast(customerid as int64) ORDER BY  COUNT(*) DESC) as rn
    FROM cricket_dataset.walmart_eu
    GROUP BY customerid, description
    ORDER BY customerid, total_purchase DESC  
)as djd
WHERE rn = 1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- Question
Find the hotel name and their total numbers of weekends bookings sort the data higher number first!
*/

CREATE TABLE cricket_dataset.bookings
(
	id INT64,
	hotel_name string,
	booking_date date,
	cust_id INT64,
	adult INT64,
	payment_type string
);

-- inserting records

INSERT INTO cricket_dataset.bookings (id, hotel_name, booking_date, cust_id, adult, payment_type) VALUES
(1, 'Hotel A', '2022-05-06', 1001, 2, 'Credit'),
(2, 'Hotel B', '2022-05-06', 1002, 1, 'Cash'),
(3, 'Hotel C', '2022-05-07', 1003, 3, 'Credit'),
(4, 'Hotel D', '2022-05-07', 1004, 2, 'Cash'),
(5, 'Hotel E', '2022-05-05', 1005, 1, 'Credit'),
(6, 'Hotel A', '2022-05-07', 1006, 2, 'Cash'),
(7, 'Hotel B', '2022-05-06', 1007, 3, 'Credit'),
(8, 'Hotel C', '2022-05-08', 1008, 1, 'Cash'),
(9, 'Hotel D', '2022-05-09', 1009, 2, 'Credit'),
(10, 'Hotel E', '2022-05-10', 1010, 3, 'Cash'),
(11, 'Hotel A', '2022-05-14', 1011, 1, 'Credit'),
(12, 'Hotel B', '2022-05-21', 1012, 2, 'Cash'),
(13, 'Hotel C', '2022-05-13', 1013, 3, 'Credit'),
(14, 'Hotel D', '2022-05-14', 1014, 1, 'Cash'),
(15, 'Hotel E', '2022-05-15', 1015, 2, 'Credit'),
(16, 'Hotel A', '2022-05-21', 1016, 3, 'Cash'),
(17, 'Hotel B', '2022-05-17', 1017, 1, 'Credit'),
(18, 'Hotel C', '2022-05-18', 1018, 2, 'Cash'),
(19, 'Hotel D', '2022-05-19', 1019, 3, 'Credit'),
(20, 'Hotel E', '2022-05-20', 1020, 1, 'Cash'),
(21, 'Hotel A', '2022-05-28', 1021, 2, 'Credit'),
(22, 'Hotel B', '2022-05-22', 1022, 3, 'Cash'),
(23, 'Hotel C', '2022-05-23', 1023, 1, 'Credit'),
(24, 'Hotel D', '2022-05-24', 1024, 2, 'Cash'),
(25, 'Hotel E', '2022-05-25', 1025, 3, 'Credit'),
(26, 'Hotel A', '2022-06-04', 1026, 1, 'Cash'),
(27, 'Hotel B', '2022-06-04', 1027, 2, 'Credit'),
(28, 'Hotel C', '2022-05-28', 1028, 3, 'Cash'),
(29, 'Hotel D', '2022-05-29', 1029, 1, 'Credit'),
(30, 'Hotel E', '2022-06-25', 1030, 2, 'Cash'),
(31, 'Hotel A', '2022-06-18', 1031, 3, 'Credit'),
(32, 'Hotel B', '2022-06-02', 1032, 1, 'Cash'),
(33, 'Hotel C', '2022-06-03', 1033, 2, 'Credit'),
(34, 'Hotel D', '2022-06-04', 1034, 3, 'Cash'),
(35, 'Hotel E', '2022-06-05', 1035, 1, 'Credit'),
(36, 'Hotel A', '2022-07-09', 1036, 2, 'Cash'),
(37, 'Hotel B', '2022-06-06', 1037, 3, 'Credit'),
(38, 'Hotel C', '2022-06-08', 1038, 1, 'Cash'),
(39, 'Hotel D', '2022-06-09', 1039, 2, 'Credit'),
(40, 'Hotel E', '2022-06-10', 1040, 3, 'Cash'),
(41, 'Hotel A', '2022-07-23', 1041, 1, 'Credit'),
(42, 'Hotel B', '2022-06-12', 1042, 2, 'Cash'),
(43, 'Hotel C', '2022-06-13', 1043, 3, 'Credit'),
(44, 'Hotel D', '2022-06-14', 1044, 1, 'Cash'),
(45, 'Hotel E', '2022-06-15', 1045, 2, 'Credit'),
(46, 'Hotel A', '2022-06-24', 1046, 3, 'Cash'),
(47, 'Hotel B', '2022-06-24', 1047, 1, 'Credit'),
(48, 'Hotel C', '2022-06-18', 1048, 2, 'Cash'),
(49, 'Hotel D', '2022-06-19', 1049, 3, 'Credit'),
(50, 'Hotel E', '2022-06-20', 1050, 1, 'Cash');

-- hotel_name,
-- total no of bookings which basically for weekends
-- Group by by hotel_name
-- order by total booking

SELECT 
    hotel_name,SUM(CASE WHEN EXTRACT(DAY FROM booking_date) IN (6, 7)THEN 1 ELSE 0 END) as total_w_bookings   
FROM cricket_dataset.bookings 
GROUP BY hotel_name
ORDER BY total_w_bookings DESC


--Find out hotel_name and their total number of booking by credit card and cash
select hotel_name,
sum(case when payment_type = 'Credit' then 1 end) as total_bookings_by_credit,
sum(case when payment_type = 'Cash' then 1 end) as total_bookings_by_cash
from cricket_dataset.bookings
group by hotel_name
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--We need to Find unique combination of records in output.
--To solve this, we will write a query by using least, greatest and Rownumber() functions.

CREATE TABLE cricket_dataset.routes (Origin string, Destination string);

INSERT INTO cricket_dataset.routes VALUES 
('Bangalore', 'Chennai'), 
('Chennai', 'Bangalore'), 
('Pune', 'Chennai'), 
('Delhi', 'Pune');

with cte as(select *,
row_number() over(partition by least(Origin,Destination),greatest(Origin,Destination) order by Origin)as rn
from cricket_dataset.routes)
select Origin,Destination from cte where rn=1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You have amazon orders data For each week, find the total number of orders. Include only the orders that are from the first quarter of 2023.
The output should contain 'week' and 'quantity'.*/

DROP TABLE IF EXISTS cricket_dataset.orders;
CREATE TABLE cricket_dataset.orders (
    order_id INT64,
    order_date DATE,
    quantity INT64
);


INSERT INTO cricket_dataset.orders 
(order_id, order_date, quantity) 
VALUES
(1, '2023-01-02', 5),
(2, '2023-02-05', 3),
(3, '2023-02-07', 2),
(4, '2023-03-10', 6),
(5, '2023-02-15', 4),
(6, '2023-04-21', 8),
(7, '2023-05-28', 7),
(8, '2023-05-05', 3),
(9, '2023-08-10', 5),
(10, '2023-05-02', 6),
(11, '2023-02-07', 4),
(12, '2023-04-15', 9),
(13, '2023-03-22', 7),
(14, '2023-04-30', 8),
(15, '2023-04-05', 6),
(16, '2023-02-02', 6),
(17, '2023-01-07', 4),
(18, '2023-05-15', 9),
(19, '2023-05-22', 7),
(20, '2023-06-30', 8),
(21, '2023-07-05', 6);

--find week no from order date
--sum(quantity) and whete order by quarter 2023
--group by week

select extract(WEEK from order_date) as week,
sum(quantity) as total_quantity_sold
from cricket_dataset.orders
where extract(YEAR from order_date)=2023 and extract(QUARTER from order_date)=1
group by week


--find each quarter and their total quantity sales
select extract(QUARTER from order_date) as Quarter_number,sum(quantity) as total_orders from cricket_dataset.orders
group by extract(QUARTER from order_date)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- Top Monthly Sellers
You are provided with a transactional dataset from Amazon that contains detailed information about sales across different products and marketplaces. 
Your task is to list the top 3 sellers in each product category for January.
The output should contain 'seller_id' , 'total_sales' ,'product_category' , 'market_place', and 'month'.
*/

CREATE TABLE cricket_dataset.sales_data (
    seller_id string,
    total_sales float64,
    product_category string,
    market_place string,
    month DATE
);



INSERT INTO cricket_dataset.sales_data (seller_id, total_sales, product_category, market_place, month)
VALUES
('s236', 36486.73, 'electronics', 'in', DATE '2024-01-01'),
('s918', 24286.4, 'books', 'uk', DATE '2024-01-01'),
('s163', 18846.34, 'electronics', 'us', DATE '2024-01-01'),
('s836', 35687.65, 'electronics', 'uk', DATE '2024-01-01'),
('s790', 31050.13, 'clothing', 'in', DATE '2024-01-01'),
('s195', 14299, 'books', 'de', DATE '2024-01-01'),
('s483', 49361.62, 'clothing', 'uk', DATE '2024-01-01'),
('s891', 48847.68, 'electronics', 'de', DATE '2024-01-01'),
('s272', 11324.61, 'toys', 'us', DATE '2024-01-01'),
('s712', 43739.86, 'toys', 'in', DATE '2024-01-01'),
('s968', 36042.66, 'electronics', 'jp', DATE '2024-01-01'),
('s728', 29158.51, 'books', 'us', DATE '2024-01-01'),
('s415', 24593.5, 'electronics', 'uk', DATE '2024-01-01'),
('s454', 35520.67, 'toys', 'in', DATE '2024-01-01'),
('s560', 27320.16, 'electronics', 'jp', DATE '2024-01-01'),
('s486', 37009.18, 'electronics', 'us', DATE '2024-01-01'),
('s749', 36277.83, 'toys', 'de', DATE '2024-01-01'),
('s798', 31162.45, 'electronics', 'in', DATE '2024-01-01'),
('s515', 26372.16, 'toys', 'in', DATE '2024-01-01'),
('s662', 22157.87, 'books', 'in', DATE '2024-01-01'),
('s919', 24963.97, 'toys', 'de', DATE '2024-01-01'),
('s863', 46652.67, 'electronics', 'us', DATE '2024-01-01'),
('s375', 18107.08, 'clothing', 'de', DATE '2024-01-01'),
('s583', 20268.34, 'toys', 'jp', DATE '2024-01-01'),
('s778', 19962.89, 'electronics', 'in', DATE '2024-01-01'),
('s694', 36519.05, 'electronics', 'in', DATE '2024-01-01'),
('s214', 18948.55, 'electronics', 'de', DATE '2024-01-01'),
('s830', 39169.01, 'toys', 'us', DATE '2024-01-01'),
('s383', 12310.73, 'books', 'in', DATE '2024-01-01'),
('s195', 45633.35, 'books', 'de', DATE '2024-01-01'),
('s196', 13643.27, 'books', 'jp', DATE '2024-01-01'),
('s796', 19637.44, 'electronics', 'jp', DATE '2024-01-01'),
('s334', 11999.1, 'clothing', 'de', DATE '2024-01-01'),
('s217', 23481.03, 'books', 'in', DATE '2024-01-01'),
('s123', 36277.83, 'toys', 'uk', DATE '2024-01-01'),
('s383', 17337.392, 'electronics', 'de', DATE '2024-02-01'),
('s515', 13998.997, 'electronics', 'jp', DATE '2024-02-01'),
('s583', 36035.539, 'books', 'jp', DATE '2024-02-01'),
('s195', 18493.564, 'toys', 'de', DATE '2024-02-01'),
('s728', 34466.126, 'electronics', 'de', DATE '2024-02-01'),
('s830', 48950.221, 'electronics', 'us', DATE '2024-02-01'),
('s483', 16820.965, 'electronics', 'uk', DATE '2024-02-01'),
('s778', 48625.281, 'toys', 'in', DATE '2024-02-01'),
('s918', 37369.321, 'clothing', 'de', DATE '2024-02-01'),
('s123', 46372.816, 'electronics', 'uk', DATE '2024-02-01'),
('s195', 18317.667, 'electronics', 'in', DATE '2024-02-01'),
('s798', 41005.313, 'books', 'in', DATE '2024-02-01'),
('s454', 39090.88, 'electronics', 'de', DATE '2024-02-01'),
('s454', 17839.314, 'toys', 'us', DATE '2024-02-01'),
('s798', 31587.685, 'toys', 'in', DATE '2024-02-01'),
('s778', 21237.38, 'books', 'jp', DATE '2024-02-01'),
('s236', 10625.456, 'toys', 'jp', DATE '2024-02-01'),
('s236', 17948.627, 'toys', 'jp', DATE '2024-02-01'),
('s749', 38453.678, 'toys', 'de', DATE '2024-02-01'),
('s790', 47052.035, 'toys', 'uk', DATE '2024-02-01'),
('s272', 34931.925, 'books', 'de', DATE '2024-02-01'),
('s375', 36753.65, 'toys', 'us', DATE '2024-02-01'),
('s214', 32449.737, 'toys', 'in', DATE '2024-02-01'),
('s163', 40431.402, 'electronics', 'in', DATE '2024-02-01'),
('s214', 30909.313, 'electronics', 'in', DATE '2024-02-01'),
('s415', 18068.768, 'electronics', 'jp', DATE '2024-02-01'),
('s836', 46302.659, 'clothing', 'jp', DATE '2024-02-01'),
('s383', 19151.927, 'electronics', 'uk', DATE '2024-02-01'),
('s863', 45218.714, 'books', 'us', DATE '2024-02-01'),
('s830', 18737.617, 'books', 'de', DATE '2024-02-01'),
('s968', 22973.801, 'toys', 'in', DATE '2024-02-01'),
('s334', 20885.29, 'electronics', 'uk', DATE '2024-02-01'),
('s163', 10278.085, 'electronics', 'de', DATE '2024-02-01'),
('s272', 29393.199, 'clothing', 'jp', DATE '2024-02-01'),
('s560', 16731.642, 'electronics', 'jp', DATE '2024-02-01'),
('s583', 38120.758, 'books', 'uk', DATE '2024-03-01'),
('s163', 22035.132, 'toys', 'uk', DATE '2024-03-01'),
('s918', 26441.481, 'clothing', 'jp', DATE '2024-03-01'),
('s334', 35374.054, 'books', 'in', DATE '2024-03-01'),
('s796', 32115.724, 'electronics', 'jp', DATE '2024-03-01'),
('s749', 39128.654, 'toys', 'in', DATE '2024-03-01'),
('s217', 35341.188, 'electronics', 'us', DATE '2024-03-01'),
('s334', 16028.702, 'books', 'us', DATE '2024-03-01'),
('s383', 44334.352, 'toys', 'in', DATE '2024-03-01'),
('s163', 42380.042, 'books', 'jp', DATE '2024-03-01'),
('s483', 16974.657, 'clothing', 'in', DATE '2024-03-01'),
('s236', 37027.605, 'electronics', 'de', DATE '2024-03-01'),
('s196', 45093.574, 'toys', 'uk', DATE '2024-03-01'),
('s486', 42688.888, 'books', 'in', DATE '2024-03-01'),
('s728', 32331.738, 'electronics', 'us', DATE '2024-03-01'),
('s123', 38014.313, 'electronics', 'us', DATE '2024-03-01'),
('s662', 45483.457, 'clothing', 'jp', DATE '2024-03-01'),
('s968', 47425.4, 'books', 'uk', DATE '2024-03-01'),
('s778', 36540.071, 'books', 'in', DATE '2024-03-01'),
('s798', 29424.55, 'toys', 'us', DATE '2024-03-01'),
('s334', 10723.015, 'toys', 'de', DATE '2024-03-01'),
('s662', 24658.751, 'electronics', 'uk', DATE '2024-03-01'),
('s163', 36304.516, 'clothing', 'us', DATE '2024-03-01'),
('s863', 20608.095, 'books', 'de', DATE '2024-03-01'),
('s214', 27375.775, 'toys', 'de', DATE '2024-03-01'),
('s334', 33076.155, 'clothing', 'in', DATE '2024-03-01'),
('s515', 32880.168, 'toys', 'us', DATE '2024-03-01'),
('s195', 48157.143, 'books', 'uk', DATE '2024-03-01'),
('s583', 23230.012, 'books', 'uk', DATE '2024-03-01'),
('s334', 13013.85, 'toys', 'jp', DATE '2024-03-01'),
('s375', 20738.994, 'electronics', 'in', DATE '2024-03-01'),
('s778', 25787.659, 'electronics', 'jp', DATE '2024-03-01'),
('s796', 36845.741, 'clothing', 'uk', DATE '2024-03-01'),
('s214', 21811.624, 'electronics', 'de', DATE '2024-03-01'),
('s334', 15464.853, 'books', 'in', DATE '2024-03-01');

--find total sales by seller_id
--where month is JAN
--select top2 sellers by each category

with cte as(
select product_category,
seller_id,sum(total_sales) as sales,
dense_rank() over(partition by product_category order by sum(total_sales) desc) as dr
from cricket_dataset.sales_data
where extract(MONTH from month)=1
group by product_category,seller_id)
select * from cte where dr<=3

--Find out Each market place and their top 3 seller based on total sale
WITH cte as(
select product_category,
seller_id,sum(total_sales) as sales,market_place,
dense_rank() over(partition by market_place order by sum(total_sales) desc) as dr
from cricket_dataset.sales_data
where extract(MONTH from month)=1
group by market_place,seller_id)
select * from cte where dr<=3
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*Netflix Data Analyst Interview Question
For each video, find how many unique users flagged it. A unique user can be identified using the combination of their first name and last name. 
Do not consider rows in which there is no flag ID.
*/

-- Create the user_flags table
CREATE TABLE cricket_dataset.user_flags (
    user_firstname string,
    user_lastname string,
    video_id string,
    flag_id string
);

-- Insert the provided records into the user_flags table
INSERT INTO cricket_dataset.user_flags (user_firstname, user_lastname, video_id, flag_id) VALUES
('Richard', 'Hasson', 'y6120QOlsfU', '0cazx3'),
('Mark', 'May', 'Ct6BUPvE2sM', '1cn76u'),
('Gina', 'Korman', 'dQw4w9WgXcQ', '1i43zk'),
('Mark', 'May', 'Ct6BUPvE2sM', '1n0vef'),
('Mark', 'May', 'jNQXAC9IVRw', '1sv6ib'),
('Gina', 'Korman', 'dQw4w9WgXcQ', '20xekb'),
('Mark', 'May', '5qap5aO4i9A', '4cvwuv'),
('Daniel', 'Bell', '5qap5aO4i9A', '4sd6dv'),
('Richard', 'Hasson', 'y6120QOlsfU', '6jjkvn'),
('Pauline', 'Wilks', 'jNQXAC9IVRw', '7ks264'),
('Courtney', '', 'dQw4w9WgXcQ', NULL),
('Helen', 'Hearn', 'dQw4w9WgXcQ', '8946nx'),
('Mark', 'Johnson', 'y6120QOlsfU', '8wwg0l'),
('Richard', 'Hasson', 'dQw4w9WgXcQ', 'arydfd'),
('Gina', 'Korman', '', NULL),
('Mark', 'Johnson', 'y6120QOlsfU', 'bl40qw'),
('Richard', 'Hasson', 'dQw4w9WgXcQ', 'ehn1pt'),
('Lopez', '', 'dQw4w9WgXcQ', 'hucyzx'),
('Greg', '', '5qap5aO4i9A', NULL),
('Pauline', 'Wilks', 'jNQXAC9IVRw', 'i2l3oo'),
('Richard', 'Hasson', 'jNQXAC9IVRw', 'i6336w'),
('Johnson', 'y6120QOlsfU', '', 'iey5vi'),
('William', 'Kwan', 'y6120QOlsfU', 'kktiwe'),
('', 'Ct6BUPvE2sM', '', NULL),
('Loretta', 'Crutcher', 'y6120QOlsfU', 'nkjgku'),
('Pauline', 'Wilks', 'jNQXAC9IVRw', 'ov5gd8'),
('Mary', 'Thompson', 'Ct6BUPvE2sM', 'qa16ua'),
('Daniel', 'Bell', '5qap5aO4i9A', 'xciyse'),
('Evelyn', 'Johnson', 'dQw4w9WgXcQ', 'xvhk6d');

-- select video_id
-- COUNT(unique users)
-- DISTINTC first and last name
-- filter the data for not null flagid
-- GROUP BY

select video_id,count(distinct concat(user_firstname,user_lastname)) as cnt_of_users
from cricket_dataset.user_flags
where flag_id is not null
group by video_id
order by 2 desc
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You have meta table with columns user_id, name, status, country
Output share of US users that are active. Active users are the ones with an "open" status in the table.

Return total users and active users and active users share for US*/

CREATE TABLE cricket_dataset.fb_active_users (
    user_id INT64,
    name string,
    status string,
    country string
);

-- Insert records into fb_active_users
INSERT INTO cricket_dataset.fb_active_users (user_id, name, status, country) VALUES
(33, 'Amanda Leon', 'open', 'Australia'),
(27, 'Jessica Farrell', 'open', 'Luxembourg'),
(18, 'Wanda Ramirez', 'open', 'USA'),
(50, 'Samuel Miller', 'closed', 'Brazil'),
(16, 'Jacob York', 'open', 'Australia'),
(25, 'Natasha Bradford', 'closed', 'USA'),
(34, 'Donald Ross', 'closed', 'China'),
(52, 'Michelle Jimenez', 'open', 'USA'),
(11, 'Theresa John', 'open', 'China'),
(37, 'Michael Turner', 'closed', 'Australia'),
(32, 'Catherine Hurst', 'closed', 'Mali'),
(61, 'Tina Turner', 'open', 'Luxembourg'),
(4, 'Ashley Sparks', 'open', 'China'),
(82, 'Jacob York', 'closed', 'USA'),
(87, 'David Taylor', 'closed', 'USA'),
(78, 'Zachary Anderson', 'open', 'China'),
(5, 'Tiger Leon', 'closed', 'China'),
(56, 'Theresa Weaver', 'closed', 'Brazil'),
(21, 'Tonya Johnson', 'closed', 'Mali'),
(89, 'Kyle Curry', 'closed', 'Mali'),
(7, 'Donald Jim', 'open', 'USA'),
(22, 'Michael Bone', 'open', 'Canada'),
(31, 'Sara Michaels', 'open', 'Denmark');

-- COUNT FILTER FOR US
-- COUNT ACTIVE users in US
-- active users/total users * 100

select count(user_id) as total_users,sum(case when status='open' then 1 else 0 end) as act_users_cnt,
round(sum(case when status='open' then 1 else 0 end)/count(user_id)*100,2) as percent_of_Act_users
from cricket_dataset.fb_active_users
where country='USA';

--Find non_active users share for China
select * from cricket_dataset.fb_active_users;

select count(user_id) as total_users,sum(case when status='closed' then 1 else 0 end) as non_act_users_cnt,
round(sum(case when status='closed' then 1 else 0 end)/count(user_id)*100,2) as percent_of_NonAct_users
from cricket_dataset.fb_active_users
where country='China'
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You are given a bank transaction data with columns bank_id, customer_id, amount_type(credit debit), transaction_amount and transaction_date

-- Write a query to find starting and ending trans amount for each customer
*/
-- Create table bank_transactions
drop table if exists cricket_dataset.bank_transactions;
CREATE TABLE cricket_dataset.bank_transactions (
    transaction_id int64,
    bank_id INT64,
    customer_id INT64,
    transaction_amount float64,
    transaction_type string,
    transaction_date DATE
);

-- Insert sample records into bank_transactions
INSERT INTO cricket_dataset.bank_transactions (transaction_id,bank_id, customer_id, transaction_amount, transaction_type, transaction_date) VALUES
(1,1, 101, 500.00, 'credit', '2024-01-01'),
(2,1, 101, 200.00, 'debit', '2024-01-02'),
(3,1, 101, 300.00, 'credit', '2024-01-05'),
(4,1, 101, 150.00, 'debit', '2024-01-08'),
(5,1, 102, 1000.00, 'credit', '2024-01-01'),
(6,1, 102, 400.00, 'debit', '2024-01-03'),
(7,1, 102, 600.00, 'credit', '2024-01-05'),
(8,1, 102, 200.00, 'debit', '2024-01-09');

-- first trans details 
-- last trans details 
-- than join these 2 trans

select * from cricket_dataset.bank_transactions;
with cte1 as( --ranking transactions by doing partitions
select *,
row_number() over(partition by customer_id order by transaction_date) as rn
from cricket_dataset.bank_transactions),
cte2 as(  --first transaction details
select customer_id,transaction_amount,transaction_date
from cte1
where rn=(select min(rn) from cte1)),
cte3 as( --last transaction details
select customer_id,transaction_amount,transaction_date
from cte1
where rn=(select max(rn) from cte1))

select cte2.customer_id,cte2.transaction_amount as first_trans_amt,cte2.transaction_date as first_trans_date,
cte3.transaction_amount as last_trans_amt,cte3.transaction_date as last_trans_date
from cte2
join cte3
on cte2.customer_id = cte3.customer_id

-- Write a query to return each cx_id and their bank balance
-- Note bank balance = Total Credit - Total_debit
with cte as(select *,case when transaction_type='credit' then transaction_amount end as credit_trans,
case when transaction_type='debit' then transaction_amount end as debit_trans
from cricket_dataset.bank_transactions),
final_cte as( select customer_id,sum(credit_trans) over(partition by customer_id) as total_credit,
sum(debit_trans) over(partition by customer_id) as total_debit
from cte)
select distinct customer_id, (total_credit-total_debit) as bank_balance from final_cte

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You have a students table with columns id, name, marks and class of students

-- Write a query to fetch students with minmum marks and maximum marks 
*/

DROP TABLE IF EXISTS cricket_dataset.students;
CREATE TABLE cricket_dataset.students (
    student_id INT64,
    student_name string,
    marks INT64,
    class string
);


INSERT INTO cricket_dataset.students (student_id, student_name, marks, class) VALUES
(1, 'John Doe', 85, 'A'),
(2, 'Jane Smith', 92, 'B'),
(3, 'Michael Johnson', 78, 'A'),
(4, 'Emily Brown', 59, 'C'),
(5, 'David Lee', 88, 'B'),
(6, 'Sarah Wilson', 59, 'A'),
(7, 'Daniel Taylor', 90, 'C'),
(8, 'Emma Martinez', 79, 'B'),
(9, 'Christopher Anderson', 87, 'A'),
(10, 'Olivia Garcia', 91, 'C'),
(11, 'James Rodriguez', 83, 'B'),
(12, 'Sophia Hernandez', 94, 'A'),
(13, 'Matthew Martinez', 76, 'C'),
(14, 'Isabella Lopez', 89, 'B'),
(15, 'Ethan Gonzalez', 80, 'A'),
(16, 'Amelia Perez', 93, 'C'),
(17, 'Alexander Torres', 77, 'B'),
(18, 'Mia Flores', 86, 'A'),
(19, 'William Sanchez', 84, 'C'),
(20, 'Ava Ramirez', 97, 'B'),
(21, 'Daniel Taylor', 75, 'A'),
(22, 'Chloe Cruz', 98, 'C'),
(23, 'Benjamin Ortiz', 89, 'B'),
(24, 'Harper Reyes', 99, 'A'),
(25, 'Ryan Stewart', 99, 'C');

--first find minimum marks and max marks using min() and max()

select min(marks) from cricket_dataset.students;
select max(marks) from cricket_dataset.students;

select * from cricket_dataset.students
where marks in (59,99)

--another approach
select * from cricket_dataset.students
where marks=(select min(marks) from cricket_dataset.students)
or marks=(select max(marks) from cricket_dataset.students)

--approach3
WITH CTE
AS
(
SELECT 
    MIN(marks) as min_marks,
    MAX(marks) as max_marks
FROM cricket_dataset.students
)
SELECT
    s.*
FROM cricket_dataset.students as s
JOIN 
CTE ON s.marks = CTE.min_marks
OR
s.marks = CTE.max_marks

--Write a SQL query to return students with maximum marks in each class
SELECT
    class,
    MAX(marks) AS max_marks
FROM
    cricket_dataset.students
GROUP BY
    class;

--appraoch2
select * from
(select *,
rank() over(partition by class order by marks desc) as rnk
from cricket_dataset.students) A
where rnk = 1
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Write an SQL script to display the immediate manager of an employee.

Given a table Employees with columns: Emp_No, Emp_Name, and Manager_Id.

The script should take an input parameter Emp_No and return the employee's name along with their immediate manager's name.

If an employee has no manager (i.e., Manager_Id is NULL), display "No Boss" for that employee.
*/

DROP TABLE IF EXISTS cricket_dataset.employees;
CREATE TABLE cricket_dataset.employees (
  Emp_No float64,
  Emp_Name string,
  Job_Name string,
  Manager_Id float64,
  HireDate DATE,
  Salary float64,
  Commission float64,  
  DeptNo float64
);

INSERT INTO cricket_dataset.employees (Emp_No, Emp_Name, Job_Name, Manager_Id, HireDate, Salary, Commission, DeptNo) VALUES
(7839, 'KING', 'PRESIDENT', NULL, '1981-11-17', 5000, NULL, 10),
(7698, 'BLAKE', 'MANAGER', 7839, '1981-05-01', 2850, NULL, 30),
(7782, 'CLARK', 'MANAGER', 7839, '1981-06-09', 2450, NULL, 10),
(7566, 'JONES', 'MANAGER', NULL, '1981-04-02', 2975, NULL, 20),
(7788, 'SCOTT', 'ANALYST', 7566, '1987-07-29', 3000, NULL, 20),
(7902, 'FORD', 'ANALYST', 7566, '1981-12-03', 3000, NULL, 20),
(7369, 'SMITH', 'CLERK', 7902, '1980-12-17', 800, NULL, 20),
(7499, 'ALLEN', 'SALESMAN', NULL, '1981-02-20', 1600, 300, 30),
(7521, 'WARD', 'SALESMAN', 7698, '1981-02-22', 1250, 500, 30),
(7654, 'MARTIN', 'SALESMAN', 7698, '1981-09-28', 1250, 1400, 30),
(7844, 'TURNER', 'SALESMAN', 7698, '1981-09-08', 1500, 0, 30),
(7876, 'ADAMS', 'CLERK', NULL, '1987-06-02', 1100, NULL, 20),
(7900, 'JAMES', 'CLERK', 7698, '1981-12-03', 950, NULL, 30),
(7934, 'MILLER', 'CLERK', 7782, '1982-01-23', 1300, NULL, 10);

--use left join to emp even though they are managers so we can replace
--null values with coalesce()

select e1.Emp_Name as emp_name,
COALESCE(e2.emp_name, 'No Boss') as manager_name
from cricket_dataset.employees e1
left join cricket_dataset.employees e2
on e1.Manager_Id=e2.Emp_No
where e1.Emp_No=7499
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--write an SQL query to retrieve employee details from each department who have a salary greater than the average salary in their department.
DROP TABLE IF EXISTS cricket_dataset.employees;

CREATE TABLE cricket_dataset.employees (
  Emp_No int64 NOT NULL,
  Emp_Name string,
  Job_Name string,
  Manager_Id float64,
  HireDate DATE,
  Salary float64,
  Commission float64,  
  Department string -- Changed from DeptNo to Department
);

INSERT INTO cricket_dataset.employees (Emp_No, Emp_Name, Job_Name, Manager_Id, HireDate, Salary, Commission, Department) VALUES
(7839, 'KING', 'PRESIDENT', NULL, '1981-11-17', 5000, NULL, 'IT'),
(7698, 'BLAKE', 'MANAGER', 7839, '1981-05-01', 2850, NULL, 'HR'),
(7782, 'CLARK', 'MANAGER', 7839, '1981-06-09', 2450, NULL, 'Marketing'),
(7566, 'JONES', 'MANAGER', 7839, '1981-04-02', 2975, NULL, 'Operations'),
(7788, 'SCOTT', 'ANALYST', 7566, '1987-07-29', 3000, NULL, 'Operations'),
(7902, 'FORD', 'ANALYST', 7566, '1981-12-03', 3000, NULL, 'Operations'),
(7369, 'SMITH', 'CLERK', 7902, '1980-12-17', 800, NULL, 'Operations'),
(7499, 'ALLEN', 'SALESMAN', 7698, '1981-02-20', 1600, 300, 'HR'),
(7521, 'WARD', 'SALESMAN', 7698, '1981-02-22', 1250, 500, 'HR'),
(7654, 'MARTIN', 'SALESMAN', 7698, '1981-09-28', 1250, 1400, 'HR'),
(7844, 'TURNER', 'SALESMAN', 7698, '1981-09-08', 1500, 0, 'HR'),
(7876, 'ADAMS', 'CLERK', 7788, '1987-06-02', 1100, NULL, 'Operations'),
(7900, 'JAMES', 'CLERK', 7698, '1981-12-03', 950, NULL, 'HR'),
(7934, 'MILLER', 'CLERK', 7782, '1982-01-23', 1300, NULL, 'Marketing'),
(7905, 'BROWN', 'SALESMAN', 7698, '1981-11-12', 1250, 1400, 'HR'),
(7906, 'DAVIS', 'ANALYST', 7566, '1987-07-13', 3000, NULL, 'Operations'),
(7907, 'GARCIA', 'MANAGER', 7839, '1981-08-12', 2975, NULL, 'IT'),
(7908, 'HARRIS', 'SALESMAN', 7698, '1981-06-21', 1600, 300, 'HR'),
(7909, 'JACKSON', 'CLERK', 7902, '1981-11-17', 800, NULL, 'Operations'),
(7910, 'JOHNSON', 'MANAGER', 7839, '1981-04-02', 2850, NULL, 'Marketing'),
(7911, 'LEE', 'ANALYST', 7566, '1981-09-28', 1250, 1400, 'Operations'),
(7912, 'MARTINEZ', 'CLERK', 7902, '1981-12-03', 1250, NULL, 'Operations'),
(7913, 'MILLER', 'MANAGER', 7839, '1981-01-23', 2450, NULL, 'HR'),
(7914, 'RODRIGUEZ', 'SALESMAN', 7698, '1981-12-03', 1500, 0, 'Marketing'),
(7915, 'SMITH', 'CLERK', 7902, '1980-12-17', 1100, NULL, 'IT'),
(7916, 'TAYLOR', 'CLERK', 7902, '1981-02-20', 950, NULL, 'Marketing'),
(7917, 'THOMAS', 'SALESMAN', 7698, '1981-02-22', 1250, 500, 'Operations'),
(7918, 'WHITE', 'ANALYST', 7566, '1981-09-28', 1300, NULL, 'IT'),
(7919, 'WILLIAMS', 'MANAGER', 7839, '1981-11-17', 5000, NULL, 'Marketing'),
(7920, 'WILSON', 'SALESMAN', 7698, '1981-05-01', 2850, NULL, 'HR'),
(7921, 'YOUNG', 'CLERK', 7902, '1981-06-09', 2450, NULL, 'Operations'),
(7922, 'ADAMS', 'ANALYST', 7566, '1987-07-13', 3000, NULL, 'HR'),
(7923, 'BROWN', 'MANAGER', 7839, '1981-08-12', 2975, NULL, 'Marketing'),
(7924, 'DAVIS', 'SALESMAN', 7698, '1981-06-21', 1600, 300, 'Operations');

--avg salary of employees in dept and comapre it within table
--example of correlated subquery

select e1.Emp_No,e1.Department,e1.Salary
from cricket_dataset.employees e1
where e1.salary > (select avg(e2.salary) from cricket_dataset.employees e2 where e2.Department=e1.Department)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a SQL query to show each product category and its return percentage. 
--return percentage = total_return by category/total_overall_return * 100
--Expected Output:
--Category: Name of the product category.Return Percentage: Percentage of returns for each category.

DROP TABLE IF EXISTS cricket_dataset.amazon_products;
CREATE TABLE cricket_dataset.amazon_products (
    product_id int64,
    product_name string,
    category string,
    price float64,
    country string
);

-- Add 25+ records with real product names for the USA
INSERT INTO cricket_dataset.amazon_products (product_id,product_name, category, price, country) VALUES
(1,'iPhone 13 Pro Max', 'Smartphones', 1099.00, 'USA'),
(2,'Samsung Galaxy S21 Ultra', 'Smartphones', 1199.99, 'USA'),
(3,'Google Pixel 6 Pro', 'Smartphones', 899.00, 'USA'),
(4,'Samsung QN90A Neo QLED TV', 'TVs', 2397.99, 'USA'),
(5,'LG OLED C1 Series', 'TVs', 1996.99, 'USA'),
(6,'Sony Bravia XR A90J', 'TVs', 2798.00, 'USA'),
(7,'Apple MacBook Pro 16-inch', 'Laptops', 2399.00, 'USA'),
(8,'Dell XPS 15', 'Laptops', 1899.99, 'USA'),
(9,'Microsoft Surface Laptop 4', 'Laptops', 1299.99, 'USA'),
(10,'Sony WH-1000XM4 Wireless Headphones', 'Headphones', 348.00, 'USA'),
(11,'Bose Noise Cancelling Headphones 700', 'Headphones', 379.00, 'USA'),
(12,'Apple AirPods Pro', 'Headphones', 249.00, 'USA'),
(13,'Samsung Odyssey G9 Gaming Monitor', 'Monitors', 1399.99, 'USA'),
(14,'Dell S2721QS 27-inch 4K Monitor', 'Monitors', 339.99, 'USA'),
(15,'LG 27GN950-B UltraGear Gaming Monitor', 'Monitors', 1296.99, 'USA'),
(16,'Canon EOS R5 Mirrorless Camera', 'Cameras', 3899.00, 'USA'),
(17,'Sony Alpha a7 III Mirrorless Camera', 'Cameras', 1998.00, 'USA'),
(18,'Nikon Z7 II Mirrorless Camera', 'Cameras', 2996.95, 'USA'),
(19,'Nintendo Switch', 'Gaming Consoles', 299.99, 'USA'),
(20,'PlayStation 5', 'Gaming Consoles', 499.99, 'USA'),
(21,'Xbox Series X', 'Gaming Consoles', 499.99, 'USA'),
(22,'Apple Watch Series 7', 'Smartwatches', 399.00, 'USA'),
(23,'Samsung Galaxy Watch 4', 'Smartwatches', 249.99, 'USA'),
(24,'Fitbit Sense', 'Smartwatches', 299.95, 'USA'),
(25,'iPhone 13 Pro Max', 'Smartphones', 1099.00, 'USA'),
(26,'Samsung Galaxy S21 Ultra', 'Smartphones', 1199.99, 'USA'),
(27,'Google Pixel 6 Pro', 'Smartphones', 899.00, 'USA'),
(28,'Samsung QN90A Neo QLED TV', 'TVs', 2397.99, 'USA'),
(29,'LG OLED C1 Series', 'TVs', 1996.99, 'USA'),
(30,'Sony Bravia XR A90J', 'TVs', 2798.00, 'USA'),
(31,'Apple MacBook Pro 16-inch', 'Laptops', 2399.00, 'USA'),
(32,'Dell XPS 15', 'Laptops', 1899.99, 'USA'),
(33,'Microsoft Surface Laptop 4', 'Laptops', 1299.99, 'USA'),
(34,'Sony WH-1000XM4 Wireless Headphones', 'Headphones', 348.00, 'USA'),
(35,'Bose Noise Cancelling Headphones 700', 'Headphones', 379.00, 'USA'),
(36,'Apple AirPods Pro', 'Headphones', 249.00, 'USA'),
(37,'Samsung Odyssey G9 Gaming Monitor', 'Monitors', 1399.99, 'USA'),
(38,'Dell S2721QS 27-inch 4K Monitor', 'Monitors', 339.99, 'USA'),
(39,'LG 27GN950-B UltraGear Gaming Monitor', 'Monitors', 1296.99, 'USA'),
(40,'Canon EOS R5 Mirrorless Camera', 'Cameras', 3899.00, 'USA'),
(41,'Sony Alpha a7 III Mirrorless Camera', 'Cameras', 1998.00, 'USA'),
(42,'Nikon Z7 II Mirrorless Camera', 'Cameras', 2996.95, 'USA'),
(43,'Nintendo Switch', 'Gaming Consoles', 299.99, 'USA'),
(44,'PlayStation 5', 'Gaming Consoles', 499.99, 'USA'),
(45,'Xbox Series X', 'Gaming Consoles', 499.99, 'USA'),
(46,'Apple Watch Series 7', 'Smartwatches', 399.00, 'USA'),
(47,'Samsung Galaxy Watch 4', 'Smartwatches', 249.99, 'USA'),
(48,'Fitbit Sense', 'Smartwatches', 299.95, 'USA'),
(49,'iPhone 13 Pro Max', 'Smartphones', 1099.00, 'USA'),
(50,'Samsung Galaxy S21 Ultra', 'Smartphones', 1199.99, 'USA'),
(51,'Google Pixel 6 Pro', 'Smartphones', 899.00, 'USA'),
(52,'Samsung QN90A Neo QLED TV', 'TVs', 2397.99, 'USA'),
(53,'LG OLED C1 Series', 'TVs', 1996.99, 'USA'),
(54,'Sony Bravia XR A90J', 'TVs', 2798.00, 'USA'),
(55,'Apple MacBook Pro 16-inch', 'Laptops', 2399.00, 'USA'),
(56,'Dell XPS 15', 'Laptops', 1899.99, 'USA'),
(57,'Microsoft Surface Laptop 4', 'Laptops', 1299.99, 'USA'),
(58,'Sony WH-1000XM4 Wireless Headphones', 'Headphones', 348.00, 'USA');


DROP TABLE IF EXISTS cricket_dataset.return_records;
CREATE TABLE cricket_dataset.return_records (
    return_id int64,
    order_id INT64,
    product_id INT64,
    return_reason string,
    return_date DATE
);

-- Add 10 more return records
INSERT INTO cricket_dataset.return_records (return_id,order_id, product_id, return_reason, return_date) VALUES
(1,1006, 7, 'Defective product', '2024-04-27'),
(2,1007, 9, 'Wrong color', '2024-04-29'),
(3,1008, 8, 'Size too small', '2024-05-01'),
(4,1009, 6, 'Not satisfied with quality', '2024-05-03'),
(5,1010, 10, 'Received wrong item', '2024-05-05'),
(6,1011, 12, 'Defective product', '2024-05-07'),
(7,1012, 11, 'Changed mind', '2024-05-09'),
(8,1013, 14, 'Item not needed', '2024-05-11'),
(9,1014, 15, 'Damaged upon arrival', '2024-05-13'),
(10,1015, 13, 'Wrong quantity', '2024-05-15'),
(11,1016, 16, 'Defective product', '2024-05-17'),
(12,1017, 17, 'Wrong size', '2024-05-19'),
(13,1018, 18, 'Received damaged', '2024-05-21'),
(14,1019, 19, 'Not as described', '2024-05-23'),
(15,1020, 20, 'Changed mind', '2024-05-25'),
(16,1021, 21, 'Item not needed', '2024-05-27'),
(17,1022, 22, 'Defective product', '2024-05-29'),
(18,1023, 23, 'Wrong color', '2024-05-31'),
(19,1024, 24, 'Received wrong item', '2024-06-02'),
(20,1025, 25, 'Size too small', '2024-06-04'),
(21,1026, 26, 'Damaged upon arrival', '2024-06-06'),
(22,1027, 27, 'Defective product', '2024-06-08'),
(23,1028, 28, 'Not satisfied with quality', '2024-06-10'),
(24,1029, 29, 'Wrong quantity', '2024-06-12'),
(25,1030, 30, 'Changed mind', '2024-06-14'),
(26,1031, 31, 'Item not needed', '2024-06-16'),
(27,1032, 32, 'Defective product', '2024-06-18'),
(28,1033, 33, 'Wrong size', '2024-06-20'),
(29,1034, 34, 'Received damaged', '2024-06-22'),
(30,1035, 35, 'Not as described', '2024-06-24'),
(31,1036, 36, 'Changed mind', '2024-06-26'),
(32,1037, 37, 'Item not needed', '2024-06-28'),
(33,1038, 38, 'Defective product', '2024-06-30'),
(34,1039, 39, 'Wrong color', '2024-07-02'),
(35,1040, 40, 'Received wrong item', '2024-07-04');

-- category name,
-- total returns
-- each category return count
-- each category return count/total returns * 100

SELECT * FROM cricket_dataset.amazon_products;
SELECT * FROM cricket_dataset.return_records;

select ap.category,round(count(r.*)/(select count(*) from cricket_dataset.return_records)*100,2) as return_percentage
from cricket_dataset.amazon_products ap
join cricket_dataset.return_records r
on ap.product_id=r.product_id
group by ap.category
order by 2 desc
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Swiggy Data Analyst Interview Question:

Write a SQL query to analyze the order patterns
throughout the day, providing insights into each hour's total orders and their respective percentages of the total orders. 

The output should include the hour, total orders,and order percentage. Order by % order in decending

%orders = hourly order/total_orders * 100
*/

DROP table if exists cricket_dataset.order_data;
-- Create the table
CREATE TABLE cricket_dataset.order_data (
    order_id int64,
    order_time TIMESTAMP,
    customer_id INT64,
    total_amount float64
);


-- Add records

-- Add records
INSERT INTO order_data (order_id,order_time, customer_id, total_amount) VALUES
    (1,'2024-03-31 08:30:00', 1001, 25.50),
    (2,'2024-03-31 09:15:00', 1002, 32.75),
    (3,'2024-03-31 10:00:00', 1003, 20.00),
    (4,'2024-03-31 11:45:00', 1004, 18.50),
    (5,'2024-03-31 12:30:00', 1005, 27.80),
    (6,'2024-03-31 13:15:00', 1006, 35.20),
    (7,'2024-03-31 14:00:00', 1007, 40.00),
    (8,'2024-03-31 15:45:00', 1008, 22.90),
    (9,'2024-03-31 16:30:00', 1009, 28.75),
    (10,'2024-03-31 17:15:00', 1010, 30.60),
    (11,'2024-03-31 18:00:00', 1011, 24.95),
    (12,'2024-03-31 19:45:00', 1012, 38.25),
    (13,'2024-03-31 20:30:00', 1013, 42.80),
    (14,'2024-03-31 21:15:00', 1014, 26.40),
    (15,'2024-03-31 22:00:00', 1015, 33.10),
    (16,'2024-03-31 23:45:00', 1016, 20.50),
    (17,'2024-03-31 00:15:00', 1017, 28.75),
    (18,'2024-03-31 01:00:00', 1018, 18.90),
    (19,'2024-03-31 22:45:00', 1019, 23.25),
    (20,'2024-03-31 22:30:00', 1020, 30.00),
    (21,'2024-03-31 22:15:00', 1021, 35.80),
    (22,'2024-03-31 23:00:00', 1022, 38.50),
    (23,'2024-03-31 06:45:00', 1023, 21.20),
    (24,'2024-03-31 09:30:00', 1024, 27.95),
    (25,'2024-03-31 23:15:00', 1025, 32.70),
    (26,'2024-03-31 09:00:00', 1026, 25.45),
    (27,'2024-03-31 10:45:00', 1027, 37.80),
    (28,'2024-03-31 21:30:00', 1028, 40.90),
    (29,'2024-03-31 23:15:00', 1029, 24.60),
    (30,'2024-03-31 13:00:00', 1030, 31.75),
    (31,'2024-03-31 22:45:00', 1031, 22.50),
    (32,'2024-03-31 22:30:00', 1032, 30.25),
    (33,'2024-03-31 23:15:00', 1033, 19.80),
    (34,'2024-03-31 23:00:00', 1034, 24.75),
    (35,'2024-03-31 20:45:00', 1035, 32.50),
    (36,'2024-03-31 20:30:00', 1036, 38.20),
    (37,'2024-03-31 20:15:00', 1037, 41.75),
    (38,'2024-03-31 22:00:00', 1038, 23.80),
    (39,'2024-03-31 22:45:00', 1039, 29.95),
    (40'2024-03-31 22:30:00', 1040, 31.60);

select extract(HOUR from order_time) as each_hours,count(order_id) as total_orders,
count(1)/(select count(*) from cricket_dataset.order_data)*100 as percent_of_orders
from cricket_dataset.order_data
group by 1
order by percent_of_orders desc

--Create a new time category as Morning, After_noon, Evening and Night And Find total orders fall into this category
/*Morning < 12 O clock
After noon between 12 and 5
Evening 5 and 8
Night > 8 */

select
(case when extract(hour from order_time)<12 then 'Morning'
 when extract(hour from order_time) between 12 and 17 then 'Afternoon'
 when extract(hour from order_time) between 17 and 20 then 'Evening'
 when extract(hour from order_time)> 20 then 'Night' end) as  time_category,
 count(order_id) as total_orders
from cricket_dataset.order_data
group by (case when extract(hour from order_time)<12 then 'Morning'
 when extract(hour from order_time) between 12 and 17 then 'Afternoon'
 when extract(hour from order_time) between 17 and 20 then 'Evening'
 when extract(hour from order_time)> 20 then 'Night' end)
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
IBM wants to analyze user purchases for Fridays in the first quarter of the year. 
Calculate the average amount users spent per order for each Friday.
Write an SQL query to find the average amount spent by users per order for each Friday in the first quarter of the year.
*/
CREATE TABLE cricket_dataset.user_purchases (
    user_id INT64,
    date DATE,
    amount_spent FLOAT64,
    day_name string
);

-- Insert records into the user_purchases table
INSERT INTO cricket_dataset.user_purchases (user_id, date, amount_spent, day_name) VALUES
(1047, '2023-01-01', 288, 'Sunday'),
(1099, '2023-01-04', 803, 'Wednesday'),
(1055, '2023-01-07', 546, 'Saturday'),
(1040, '2023-01-10', 680, 'Tuesday'),
(1052, '2023-01-13', 889, 'Friday'),
(1052, '2023-01-13', 596, 'Friday'),
(1016, '2023-01-16', 960, 'Monday'),
(1023, '2023-01-17', 861, 'Tuesday'),
(1010, '2023-01-19', 758, 'Thursday'),
(1013, '2023-01-19', 346, 'Thursday'),
(1069, '2023-01-21', 541, 'Saturday'),
(1030, '2023-01-22', 175, 'Sunday'),
(1034, '2023-01-23', 707, 'Monday'),
(1019, '2023-01-25', 253, 'Wednesday'),
(1052, '2023-01-25', 868, 'Wednesday'),
(1095, '2023-01-27', 424, 'Friday'),
(1017, '2023-01-28', 755, 'Saturday'),
(1010, '2023-01-29', 615, 'Sunday'),
(1063, '2023-01-31', 534, 'Tuesday'),
(1019, '2023-02-03', 185, 'Friday'),
(1019, '2023-02-03', 995, 'Friday'),
(1092, '2023-02-06', 796, 'Monday'),
(1058, '2023-02-09', 384, 'Thursday'),
(1055, '2023-02-12', 319, 'Sunday'),
(1090, '2023-02-15', 168, 'Wednesday'),
(1090, '2023-02-18', 146, 'Saturday'),
(1062, '2023-02-21', 193, 'Tuesday'),
(1023, '2023-02-24', 259, 'Friday'),
(1023, '2023-02-24', 849, 'Friday'),
(1009, '2023-02-27', 552, 'Monday'),
(1012, '2023-03-02', 303, 'Thursday'),
(1001, '2023-03-05', 317, 'Sunday'),
(1058, '2023-03-08', 573, 'Wednesday'),
(1001, '2023-03-11', 531, 'Saturday'),
(1034, '2023-03-14', 440, 'Tuesday'),
(1096, '2023-03-17', 650, 'Friday'),
(1048, '2023-03-20', 711, 'Monday'),
(1089, '2023-03-23', 388, 'Thursday'),
(1001, '2023-03-26', 353, 'Sunday'),
(1016, '2023-03-29', 833, 'Wednesday');

--find week,day and year from date , then group by week

select extract(WEEK from date) as week_no,
avg(amount_spent) as avg_amt
from cricket_dataset.user_purchases
where 
extract(YEAR from date) =2023
and extract(QUARTER from date)=1
and extract(DAY from date)=5
group by 1
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You are given a uber_ride table with columns
ride_id, ride_time_stamp, ride_status.
(which has information about the ride)
Find out % of ride cancelled by uber_driver*/
DROP TABLE IF EXISTS cricket_dataset.uber_ride;

-- Create the Uber ride table
CREATE TABLE cricket_dataset.uber_ride (
    ride_id int64,
    ride_timestamp TIMESTAMP,
    ride_status string  -- "ride_completed", "cancelled_by_driver" or "cancelled_by_user"
);

-- Insert sample records
INSERT INTO cricket_dataset.uber_ride (ride_id,ride_timestamp, ride_status)
VALUES
    (1,'2024-05-09 08:30:00', 'cancelled_by_driver'),
    (2,'2024-05-09 09:00:00', 'cancelled_by_user'),
    (3,'2024-05-09 10:00:00', 'ride_completed'),
    (4,'2024-05-09 11:00:00', 'cancelled_by_user'),
    (5,'2024-05-09 12:00:00', 'cancelled_by_driver'),
    (6,'2024-05-09 13:00:00', 'cancelled_by_user'),
    (7,'2024-05-09 14:00:00', 'cancelled_by_user'),
    (8,'2024-05-09 15:00:00', 'cancelled_by_user'),
    (9,'2024-05-09 16:00:00', 'ride_completed'),
    (10,'2024-05-09 17:00:00', 'cancelled_by_user'),
    (11,'2024-05-09 18:00:00', 'ride_completed'),
    (12,'2024-05-09 19:00:00', 'cancelled_by_user'),
    (13,'2024-05-09 20:00:00', 'cancelled_by_user'),
    (14,'2024-05-09 21:00:00', 'cancelled_by_user'),
    (15,'2024-05-09 22:00:00', 'cancelled_by_driver'),
    (16,'2024-05-09 13:00:00', 'cancelled_by_user'),
    (17,'2024-05-09 14:00:00', 'cancelled_by_user'),
    (18,'2024-05-09 15:00:00', 'cancelled_by_user'),
    (19,'2024-05-09 16:00:00', 'ride_completed'),
    (20,'2024-05-09 17:00:00', 'cancelled_by_user'),
    (21,'2024-05-09 18:00:00', 'cancelled_by_driver'),
    (22,'2024-05-09 19:00:00', 'cancelled_by_user'),
    (23,'2024-05-09 20:00:00', 'cancelled_by_user'),
    (24,'2024-05-09 21:00:00', 'cancelled_by_user'),
    (25,'2024-05-09 22:00:00', 'cancelled_by_driver');

--total count of cancelled rides
--total cancelled by drivers
--then calculate percentage

select round(sum(case when ride_status='cancelled_by_user' then 1 else 0 end)/(select count(*) from cricket_dataset.uber_ride where ride_status != 'ride_completed')*100,2) as percentage_of_cancelled_rides
from cricket_dataset.uber_ride

--how many ride were cancelled by user in the evening. hour > 17 is considered as evening 
select * from cricket_dataset.uber_ride
where extract(hour from ride_timestamp)>17
and ride_status = 'cancelled_by_user'
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- Most Profitable Companies You are given a table called forbes_global with columns company, sector, industry, country, 
sales, profit, rank

Find out each country's most most profitable company details
*/


CREATE TABLE cricket_dataset.forbes_global (
    company string,
    sector string,
    industry string,
    country string,
    sales FLOAT64,
    profits FLOAT64,
    rank1 INT64
);

insert into cricket_dataset.forbes_global
VALUES  
('Walmart', 'Consumer Discretionary', 'General Merchandisers', 'United States', 482130.0, 14694.0, 1),
('Sinopec-China Petroleum', 'Energy', 'Oil & Gas Operations', 'China', 448452.0, 7840.0, 2),
('Royal Dutch Shell', 'Energy', 'Oil & Gas Operations', 'Netherlands', 396556.0, 15340.0, 3),
('China National Petroleum', 'Energy', 'Oil & Gas Operations', 'China', 392976.0, 2837.0, 4),
('State Grid', 'Utilities', 'Electric Utilities', 'China', 387056.0, 9573.0, 5),
('Saudi Aramco', 'Energy', 'Oil & Gas Operations', 'Saudi Arabia', 355905.0, 11002.0, 6),
('Volkswagen', 'Consumer Discretionary', 'Auto & Truck Manufacturers', 'Germany', 283565.0, 17742.4, 7),
('BP', 'Energy', 'Oil & Gas Operations', 'United Kingdom', 282616.0, 3591.0, 8),
('Amazon.com', 'Consumer Discretionary', 'Internet Services and Retailing', 'United States', 280522.0, 5362.0, 9),
('Toyota Motor', 'Consumer Discretionary', 'Auto & Truck Manufacturers', 'Japan', 275288.0, 18499.3, 10),
('Apple', 'Information Technology', 'Computers, Office Equipment', 'United States', 265595.0, 55256.0, 11),
('Exxon Mobil', 'Energy', 'Oil & Gas Operations', 'United States', 263910.0, 15850.0, 12),
('Berkshire Hathaway', 'Financials', 'Diversified Financials', 'United States', 247837.0, 8971.0, 13),
('Samsung Electronics', 'Information Technology', 'Electronics', 'South Korea', 245898.0, 19783.3, 14),
('McKesson', 'Health Care', 'Health Care: Pharmacy and Other Services', 'United States', 231091.0, 5070.0, 15),
('Glencore', 'Materials', 'Diversified Metals & Mining', 'Switzerland', 219754.0, 5436.0, 16),
('UnitedHealth Group', 'Health Care', 'Health Care: Insurance and Managed Care', 'United States', 201159.0, 13650.0, 17),
('Daimler', 'Consumer Discretionary', 'Auto & Truck Manufacturers', 'Germany', 197515.0, 8245.1, 18),
('CVS Health', 'Health Care', 'Health Care: Pharmacy and Other Services', 'United States', 194579.0, 6634.0, 19),
('AT&T', 'Telecommunication Services', 'Telecommunications', 'United States', 181193.0, 13906.0, 20),
('Foxconn', 'Technology', 'Electronics', 'Taiwan', 175617.0, 4103.4, 21),
('General Motors', 'Consumer Discretionary', 'Auto & Truck Manufacturers', 'United States', 174049.0, 6710.0, 22),
('Verizon Communications', 'Telecommunication Services', 'Telecommunications', 'United States', 170756.0, 19225.0, 23),
('Total', 'Energy', 'Oil & Gas Operations', 'France', 149769.0, 7480.0, 24),
('IBM', 'Information Technology', 'Information Technology Services', 'United States', 141682.0, 6606.0, 25),
('Ford Motor', 'Consumer Discretionary', 'Auto & Truck Manufacturers', 'United States', 140545.0, 6471.0, 26),
('Hon Hai Precision Industry', 'Technology', 'Electronics', 'Taiwan', 135129.0, 4493.3, 27),
('Trafigura Group', 'Energy', 'Trading', 'Singapore', 131638.0, 975.0, 28),
('General Electric', 'Industrials', 'Diversified Industrials', 'United States', 126661.0, 5136.0, 29),
('AmerisourceBergen', 'Health Care', 'Wholesalers: Health Care', 'United States', 122848.0, 1605.5, 30),
('Fannie Mae', 'Financials', 'Diversified Financials', 'United States', 120472.0, 18418.0, 31),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 120438.0, 975.0, 32),
('Koch Industries', 'Diversified', 'Diversified', 'United States', 115095.0, 5142.0, 33),
('Cardinal Health', 'Health Care', 'Wholesalers: Health Care', 'United States', 113982.0, 1377.0, 34),
('Alphabet', 'Technology', 'Internet Services and Retailing', 'United States', 110855.0, 18616.0, 35),
('Chevron', 'Energy', 'Oil & Gas Operations', 'United States', 110360.0, 5520.0, 36),
('Costco Wholesale', 'Consumer Discretionary', 'General Merchandisers', 'United States', 110215.0, 2115.0, 37),
('Cardinal Health', 'Health Care', 'Health Care: Pharmacy and Other Services', 'United States', 109838.0, 1718.0, 38),
('Ping An Insurance Group', 'Financials', 'Insurance', 'China', 109254.0, 2047.4, 39),
('Walgreens Boots Alliance', 'Consumer Staples', 'Food and Drug Stores', 'United States', 109026.0, 4563.0, 40),
('Costco Wholesale', 'Consumer Discretionary', 'Retailing', 'United States', 105156.0, 2115.0, 41),
('JPMorgan Chase', 'Financials', 'Diversified Financials', 'United States', 105153.0, 30615.0, 42),
('Verizon Communications', 'Telecommunication Services', 'Telecommunications', 'United States', 104887.0, 13568.0, 43),
('China Construction Bank', 'Financials', 'Banks', 'China', 104693.0, 38369.0, 44),
('China Construction Bank', 'Financials', 'Major Banks', 'China', 104692.9, 38369.2, 45),
('Trafigura Group', 'Energy', 'Trading', 'Netherlands', 103752.0, 975.0, 46),
('Exor Group', 'Financials', 'Diversified Financials', 'Netherlands', 103606.6, -611.2, 47),
('Anheuser-Busch InBev', 'Consumer Staples', 'Beverages', 'Belgium', 101541.0, 9536.0, 48),
('Bank of America', 'Financials', 'Banks', 'United States', 100264.0, 18724.0, 49),
('Bank of China', 'Financials', 'Banks', 'China', 99237.3, 28202.1, 50),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 97296.0, 975.0, 51),
('Dell Technologies', 'Technology', 'Computers, Office Equipment', 'United States', 94477.0, 2743.0, 52),
('CVS Health', 'Health Care', 'Health Care: Insurance and Managed Care', 'United States', 94005.0, 6239.0, 53),
('Trafigura Group', 'Energy', 'Trading', 'United Kingdom', 90345.0, 975.0, 54),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 88265.0, 975.0, 55),
('Trafigura Group', 'Energy', 'Trading', 'Netherlands', 88111.0, 975.0, 56),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 87044.0, 975.0, 57),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 84795.0, 975.0, 58),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 84361.0, 975.0, 59),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 83156.0, 975.0, 60),
('Trafigura Group', 'Energy', 'Trading', 'Switzerland', 82276.0, 975.0, 61);

select * from(
select *,
rank() over(partition by country order by profits) as rn
from cricket_dataset.forbes_global)
where rn=1

--Find out each sector top 2 most profitable company details
select * from(
select *,
dense_rank() over(partition by industry order by profits) as rn
from cricket_dataset.forbes_global)
where rn<=2
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You are given a table of New York housing  price called house_transactions with columns id, state, city, street_address, mkt_price

Identify properites where the mkt_price of the house exceeds the city's average mkt_price.*/

-- Create the table
CREATE TABLE cricket_dataset.house_price (
    id INT64,
    state string,
    city string,
    street_address string,
    mkt_price INT64
);
-- Insert all the records
INSERT INTO cricket_dataset.house_price (id, state, city, street_address, mkt_price) VALUES
(1, 'NY', 'New York City', '66 Trout Drive', 449761),
(2, 'NY', 'New York City', 'Atwater', 277527),
(3, 'NY', 'New York City', '58 Gates Street', 268394),
(4, 'NY', 'New York City', 'Norcross', 279929),
(5, 'NY', 'New York City', '337 Shore Ave.', 151592),
(6, 'NY', 'New York City', 'Plainfield', 624531),
(7, 'NY', 'New York City', '84 Central Street', 267345),
(8, 'NY', 'New York City', 'Passaic', 88504),
(9, 'NY', 'New York City', '951 Fulton Road', 270476),
(10, 'NY', 'New York City', 'Oxon Hill', 118112),
(11, 'CA', 'Los Angeles', '692 Redwood Court', 150707),
(12, 'CA', 'Los Angeles', 'Lewiston', 463180),
(13, 'CA', 'Los Angeles', '8368 West Acacia Ave.', 538865),
(14, 'CA', 'Los Angeles', 'Pearl', 390896),
(15, 'CA', 'Los Angeles', '8206 Old Riverview Rd.', 117754),
(16, 'CA', 'Los Angeles', 'Seattle', 424588),
(17, 'CA', 'Los Angeles', '7227 Joy Ridge Rd.', 156850),
(18, 'CA', 'Los Angeles', 'Battle Ground', 643454),
(19, 'CA', 'Los Angeles', '233 Bedford Ave.', 713841),
(20, 'CA', 'Los Angeles', 'Saint Albans', 295852),
(21, 'IL', 'Chicago', '8830 Baker St.', 12944),
(22, 'IL', 'Chicago', 'Watertown', 410766),
(23, 'IL', 'Chicago', '632 Princeton St.', 160696),
(24, 'IL', 'Chicago', 'Waxhaw', 464144),
(25, 'IL', 'Chicago', '7773 Tailwater Drive', 129393),
(26, 'IL', 'Chicago', 'Bonita Springs', 174886),
(27, 'IL', 'Chicago', '31 Summerhouse Rd.', 296008),
(28, 'IL', 'Chicago', 'Middleburg', 279000),
(29, 'IL', 'Chicago', '273 Windfall Avenue', 424846),
(30, 'IL', 'Chicago', 'Graham', 592268),
(31, 'TX', 'Houston', '91 Canterbury Dr.', 632014),
(32, 'TX', 'Houston', 'Dallas', 68868),
(33, 'TX', 'Houston', '503 Elmwood St.', 454184),
(34, 'TX', 'Houston', 'Kennewick', 186280),
(35, 'TX', 'Houston', '739 Chapel Street', 334474),
(36, 'TX', 'Houston', 'San Angelo', 204460),
(37, 'TX', 'Houston', '572 Parker Dr.', 678443),
(38, 'TX', 'Houston', 'Bellmore', 401090),
(39, 'TX', 'Houston', '8653 South Oxford Street', 482214),
(40, 'TX', 'Houston', 'Butler', 330868),
(41, 'AZ', 'Phoenix', '8667 S. Joy Ridge Court', 316291),
(42, 'AZ', 'Phoenix', 'Torrance', 210392),
(43, 'AZ', 'Phoenix', '35 Harvard St.', 167502),
(44, 'AZ', 'Phoenix', 'Nutley', 327554),
(45, 'AZ', 'Phoenix', '7313 Vermont St.', 285135),
(46, 'AZ', 'Phoenix', 'Lemont', 577667),
(47, 'AZ', 'Phoenix', '8905 Buttonwood Dr.', 212301),
(48, 'AZ', 'Phoenix', 'Lafayette', 317504);

--solutin for only new york city
SELECT id,state,city,mkt_price
FROM cricket_dataset.house_price
where mkt_price > (select avg(mkt_price) from cricket_dataset.house_price where city='New York City')

--using correlated query
SELECT h1.id,h1.state,h1.city,h1.mkt_price
FROM cricket_dataset.house_price h1
where h1.mkt_price > (select avg(h2.mkt_price) from cricket_dataset.house_price h2 where h2.city=h1.city)

--Write a query to find the property that has house mkt_price greater 
-- than average of the city's average price but less than nation's average price
SELECT h1.id,h1.state,h1.city,h1.mkt_price
FROM cricket_dataset.house_price h1
where h1.mkt_price > (select avg(h2.mkt_price) from cricket_dataset.house_price h2 where h2.city=h1.city) and
h1.mkt_price < (select avg(h.mkt_price)from house_price as h);
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- Amazon Data Analyst interview questions for exp 1-3 year!

You have two tables: Orders and Customers.

 - Orders Table Columns: 
Order_id, Customer_id, Order_Date, Amount

- Customers Table Columns: 
Customer_id, Customer_Name, Join_Date
 
Write an SQL query to calculate the total order amount for each customer who joined 
in the current year. 

The output should contain Customer_Name and the total amount.
*/
DROP TABLE IF EXISTS cricket_dataset.orders;
-- Create Orders table
CREATE TABLE cricket_dataset.orders (
    Order_id INT64,
    Customer_id INT64,
    Order_Date DATE,
    Amount float64
);

DROP TABLE IF EXISTS cricket_dataset.customers;
-- Create Customers table
CREATE TABLE cricket_dataset.customers (
    Customer_id INT64,
    Customer_Name string,
    Join_Date DATE
);

-- Insert records into Orders table
INSERT INTO cricket_dataset.orders (Order_id, Customer_id, Order_Date, Amount)
VALUES
    (1, 1, '2024-05-01', 100),
    (2, 2, '2024-05-02', 150),
    (3, 3, '2023-12-15', 200),
    (4, 1, '2024-05-03', 120),
    (5, 2, '2024-01-20', 180),
    (6, 4, '2024-03-10', 90);

-- Insert records into Customers table
INSERT INTO cricket_dataset.customers (Customer_id, Customer_Name, Join_Date)
VALUES
    (1, 'Alice', '2024-01-15'),
    (2, 'Bob', '2024-02-20'),
    (3, 'Charlie', '2023-12-01'),
    (4, 'David', '2024-03-01');

-- join both table based on cx id
-- filter the cx records for current_year 
-- based on eacx cx id sum the amount
-- group by cx id

select c.Customer_id,c.Customer_Name,sum(o.Amount) as total_sum from cricket_dataset.customers c
join cricket_dataset.orders o
on c.Customer_id=o.Customer_id
where extract(YEAR from c.Join_Date) = extract(YEAR from current_date)
group by Customer_id,Customer_Name
order by Customer_id

--Write a SQL query to return each and total orders for current year
-- return month_number, total orders
select extract(MONTH from Order_Date) as month_number,count(Order_id) as total_orders from cricket_dataset.orders
where extract(YEAR from Order_Date) = extract(YEAR from current_date)
group by extract(MONTH from Order_date)
order by 1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Write an SQL query to display inventory details including the product name, quantity in stock, remaining stock level ('Medium' if quantity is
more than 10, 'Low' otherwise), and supplier ID. 

Assume each product has a unique supplier ID associated with it.
*/

DROP TABLE IF EXISTS cricket_dataset.inventory;
CREATE TABLE cricket_dataset.inventory (
    product_id int64,
    product_name string,
    quantity INT64,
    price_per_unit FLOAT64
);


INSERT INTO cricket_dataset.inventory (product_id,product_name, quantity, price_per_unit)
VALUES
    (1,'Laptop', 20, 999.99),
    (2,'Smartphone', 15, 699.99),
    (3,'Tablet', 8, 399.99),
    (4,'Headphones', 25, 149.99),
    (5,'Mouse', 30, 29.99),
    (6,'Wireless Earbuds', 12, 79.99),
    (7,'Portable Charger', 10, 49.99),
    (8,'Bluetooth Speaker', 18, 129.99),
    (9,'Fitness Tracker', 7, 89.99),
    (10,'External Hard Drive', 9, 149.99),
    (11,'Gaming Mouse', 14, 59.99),
    (12,'USB-C Cable', 22, 19.99),
    (13,'Smart Watch', 6, 199.99),
    (14,'Desk Lamp', 11, 34.99),
    (15,'Power Bank', 16, 39.99),
    (16,'Wireless Mouse', 13, 29.99),
    (17,'Bluetooth Headset', 20, 59.99),
    (18,'MicroSD Card', 5, 24.99),
    (19,'USB Flash Drive', 8, 14.99),
    (20,'HDMI Cable', 17, 9.99);


-- product name, quantity in stock, stock level
-- qty > 10 medium, Low
-- supplier ID

select *,
case when quantity>10 then 'Medium' else 'Low' end as Category_Label
from cricket_dataset.inventory
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*You are given customers and orders table with columns customers has columns cx_id, cx_name, city, country
orders table has COLUMNS order_id, cx_id, order_date, totalamount

Write an SQL query to retrive customer details along with their total order amounts(if any).
Include the customer's name, city, country, and total order amount. 
If a customer hasn't placed any orders,display 'NULL' for the total order amount."*/

DROP TABlE IF EXISTS cricket_dataset.customers;
DROP TABlE IF EXISTS cricket_dataset.orders;


CREATE TABLE cricket_dataset.customers (
    CustomerID INT64,
    CustomerName string,
    City string,
    Country string
);

CREATE TABLE cricket_dataset.orders (
    OrderID INT64,
    CustomerID INT64,
    OrderDate DATE,
    TotalAmount float64,
);

-- Insert records into the 'Customers' table
INSERT INTO cricket_dataset.customers (CustomerID, CustomerName, City, Country) 
VALUES 
(1, 'John Doe', 'New York', 'USA'),
(2, 'Jane Smith', 'Los Angeles', 'USA'),
(3, 'Michael Johnson', 'Chicago', 'USA'),
(4, 'Emily Brown', 'Houston', 'USA');

-- Insert records into the 'Orders' table
INSERT INTO cricket_dataset.orders (OrderID, CustomerID, OrderDate, TotalAmount) 
VALUES 
(101, 1, '2024-05-10', 150.00),
(102, 2, '2024-05-11', 200.00),
(103, 1, '2024-05-12', 100.00),
(104, 3, '2024-05-13', 300.00);


SELECT * FROM cricket_dataset.customers;
SELECT * FROM cricket_dataset.orders;

--do left join

SELECT c.*,
sum(o.TotalAmount) as total
FROM cricket_dataset.customers c
left join cricket_dataset.orders o
on c.CustomerID=o.CustomerID
group by 1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
You are given a orders table with columns order_id, order_date
Identify the busiest day for orders along with the total number of orders placed on that day. 
*/

DROP TABLE IF EXISTS cricket_dataset.orders;
-- Create table for orders
CREATE TABLE cricket_dataset.orders (
    order_id int64,
    order_date DATE
);

-- Insert sample records for orders
INSERT INTO cricket_dataset.orders (order_id,order_date)
VALUES
    (1,'2024-05-01'),
    (2,'2024-05-01'),
    (3,'2024-05-01'),
    (4,'2024-05-02'),
    (5,'2024-05-02'),
    (6,'2024-05-02'),
    (7,'2024-05-03'),
    (8,'2024-05-03'),
    (9,'2024-05-03'),
    (10,'2024-05-03'),
    (11,'2024-05-03'),
    (12,'2024-05-04'),
    (13,'2024-05-04'),
    (14,'2024-05-04'),
    (15,'2024-05-04'),
    (16,'2024-05-04'),
    (17,'2024-05-05'),
    (18,'2024-05-05'),
    (19,'2024-05-05'),
    (20,'2024-05-05'),
    (21,'2024-05-06'),
    (22,'2024-05-06'),
    (23,'2024-05-06'),
    (24,'2024-05-06'),
    (25,'2024-05-06');

--just group by order_date
select order_date,count(*) as total_orders from cricket_dataset.orders
group by order_date

--for max orders on which day
select order_date,count(*) as total_orders from cricket_dataset.orders
group by order_date
order by order_date desc
limit 1

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- You have two tables, sellers and orders. 
sellers table columns are seller_id, seller_name
orders table has columns seller_id, product_id, category, quantity, price_per_unit
Write an SQL query to find each seller's revenue from each category and each product.
return seller_name, total_revenue in each product inside each category
*/

DROP TABLE IF EXISTS cricket_dataset.sellers;
DROP TABLE IF EXISTS cricket_dataset.orders;
-- Create table for sellers
CREATE TABLE cricket_dataset.sellers (
    seller_id int64,
    seller_name string
);

-- Insert sample records into the sellers table
INSERT INTO cricket_dataset.sellers (seller_id,seller_name)
VALUES 
    (1,'Seller A'),
    (2,'Seller B'),
    (3,'Seller C');

-- Create table for orders
CREATE TABLE cricket_dataset.orders (
    order_id int64,
    seller_id INT64,
    product_id INT64,
    category string,
    quantity INT64,
    price_per_unit FLOAT64
);

-- Insert sample records into the orders table
INSERT INTO cricket_dataset.orders (order_id,seller_id, product_id, category, quantity, price_per_unit)
VALUES 
    (1,1, 1, 'Electronics', 2, 999.99),
    (2,1, 2, 'Electronics', 3, 699.99),
    (3,2, 3, 'Home & Kitchen', 1, 49.99),
    (4,2, 4, 'Home & Kitchen', 2, 79.99),
    (5,2, 5, 'Electronics', 1, 29.99),
    (6,3, 1, 'Electronics', 2, 999.99),
    (7,3, 4, 'Home & Kitchen', 1, 79.99),
    (8,1, 3, 'Home & Kitchen', 2, 49.99),
    (9,2, 1, 'Electronics', 1, 999.99),
    (10,3, 2, 'Electronics', 1, 699.99),
    (11,1, 4, 'Home & Kitchen', 3, 79.99),
    (12,2, 2, 'Electronics', 2, 699.99),
    (13,3, 3, 'Home & Kitchen', 1, 49.99),
    (14,1, 5, 'Electronics', 2, 29.99),
    (15,2, 4, 'Home & Kitchen', 1, 79.99),
    (16,3, 1, 'Electronics', 1, 999.99),
    (17,1, 2, 'Electronics', 1, 699.99),
    (18,2, 3, 'Home & Kitchen', 2, 49.99),
    (19,3, 5, 'Electronics', 1, 29.99),
    (20,1, 3, 'Home & Kitchen', 1, 49.99),
    (21,2, 1, 'Electronics', 3, 999.99),
    (22,3, 2, 'Electronics', 2, 699.99),
    (23,1, 4, 'Home & Kitchen', 1, 79.99),
    (24,2, 2, 'Electronics', 1, 699.99),
    (25,3, 3, 'Home & Kitchen', 3, 49.99),
    (26,1, 5, 'Electronics', 1, 29.99);

-- seller_name get from seller_table
-- category_name orders TABLE
-- product_id
-- revenue    

select s.seller_name,o.category,o.product_id,sum(o.price_per_unit*o.quantity) as total_rev
from cricket_dataset.orders o
join cricket_dataset.sellers s
on s.seller_id=o.seller_id
group by 1,2,3
order by 1,3
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
You have orders table with columns order_id, customer_id, order_date, total_amount
Calculate the running total of orders for each customer. 
Return the customer ID, order date, total amount of each order, and the cumulative total of orders for each customer sorted by customer ID and order date.
*/
DROP TABLE IF EXISTS cricket_dataset.orders;
-- Create table
CREATE TABLE cricket_dataset.orders (
    order_id int64,
    customer_id INT64,
    order_date DATE,
    total_amount FLOAT64
);

-- Insert records
INSERT INTO cricket_dataset.orders (order_id,customer_id, order_date, total_amount)
VALUES
    (1,1001, '2024-01-01', 120.25),
    (2,1002, '2024-01-03', 80.99),
    (3,1003, '2024-01-05', 160.00),
    (4,1004, '2024-01-07', 95.50),
    (5,1001, '2024-02-09', 70.75),
    (6,1002, '2024-02-11', 220.00),
    (7,1003, '2024-02-13', 130.50),
    (8,1004, '2024-02-15', 70.25),
    (9,1001, '2024-02-17', 60.75),
    (10,1002, '2024-03-19', 180.99),
    (11,1003, '2024-03-21', 140.00),
    (12,1004, '2024-03-23', 110.50),
    (13,1001, '2024-03-25', 90.25),
    (14,1002, '2024-03-27', 200.00),
    (15,1003, '2024-03-29', 160.50),
    (16,1004, '2024-03-31', 120.75),
    (17,1001, '2024-03-02', 130.25),
    (18,1002, '2024-03-04', 90.99),
    (19,1003, '2024-03-06', 170.00),
    (20,1004, '2024-04-08', 105.50),
    (21,1001, '2024-04-10', 80.75),
    (22,1002, '2024-04-12', 240.00),
    (23,1003, '2024-04-14', 150.50),
    (24,1004, '2024-04-16', 80.25),
    (25,1001, '2024-04-18', 70.75);

--use partition by
select *,
sum(total_amount) over(partition by customer_id order by order_date desc) as running_sum
from cricket_dataset.orders

---- Find each customer_id and revenue collected from them in each month
select customer_id,extract(month from order_date) as month_number,sum(total_amount) as revenue_collected
from cricket_dataset.orders
group by customer_id,2
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Questions using lead and lag concepts.

--Write a query to compare each employee's salary with the previous employee's salary based on their employee_id.
--Basic LAG() Query
select employee_id,salaryq, 
lag(salary) over(order by employee_id) as previous_salary
from employees;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to find the next order date for each customer based on their order history.
--Basic LEAD() Query
SELECT 
    customer_id,
    order_date,
    LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_order_date
FROM orders;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to calculate the difference in sales between the current and the previous month for each product.
--compare concurrent and previous month sales
SELECT 
    product_id,
    sales_month,
    sales_amount,
    LAG(sales_amount) OVER (PARTITION BY product_id ORDER BY sales_month) AS previous_month_sales,
    sales_amount - LAG(sales_amount) OVER (PARTITION BY product_id ORDER BY sales_month) AS sales_difference
FROM monthly_sales;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to find employees whose current salary is higher than their previous salary
SELECT 
    employee_id,
    salary,
    LAG(salary) OVER (PARTITION BY department_id ORDER BY hire_date) AS previous_salary
FROM employees
WHERE salary > LAG(salary) OVER (PARTITION BY department_id ORDER BY hire_date);
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to calculate the time difference (in hours) between consecutive log entries for each user.
--find the time difference between consecutive events
SELECT 
    user_id,
    event_time,
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_event_time,
    EXTRACT(EPOCH FROM event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)) / 3600 AS time_difference_hours
FROM event_logs;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Write a query to flag rows as "start" or "end" of an activity period, based on consecutive timestamps for a user. Assume there's no activity if the time gap is greater than 1 hour.
--flag start and end of activity periods
SELECT 
    user_id,
    event_time,
    CASE 
        WHEN LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL 
             OR EXTRACT(EPOCH FROM event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)) / 3600 > 1 
        THEN 'start'
        ELSE NULL
    END AS period_start,
    CASE 
        WHEN LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL 
             OR EXTRACT(EPOCH FROM LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time) - event_time) / 3600 > 1 
        THEN 'end'
        ELSE NULL
    END AS period_end
FROM event_logs;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Calculate Running Total with Dynamic LAG
--Write a query to calculate the difference in sales between the current date and the last date where sales exceeded 1000 for each product.
SELECT 
    product_id,
    sale_date,
    sales_amount,
    sale_date - LAG(sale_date) OVER (PARTITION BY product_id ORDER BY sale_date) AS days_since_last_high_sales,
    sales_amount - LAG(sales_amount) OVER (PARTITION BY product_id ORDER BY sale_date) AS difference_from_last_high_sales
FROM (
    SELECT 
        product_id, 
        sale_date, 
        sales_amount, 
        CASE WHEN sales_amount > 1000 THEN sale_date ELSE NULL END AS high_sales_date
    FROM product_sales
) AS subquery;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

