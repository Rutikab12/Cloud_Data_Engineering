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
