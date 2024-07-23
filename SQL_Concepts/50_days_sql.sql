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
