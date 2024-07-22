Important Interview SQL Questions

--find the duplicates in a table
select emp_id, count(1) as rn from Employees group by emp_id where rn>1

--delete those duplicates
with ctn as(
	select *, row_number() over(partition by emp_id order by emp_id) as rn from emp1
)
delete from cte where rn>1

--difference between union and union all 
union will have only unique records no duplicate is allowed
while union all have all the records including duplicates

select manaeger_id from emp1
union/union all
select manager_id from emp;

--select employees who are not in the department table
select * from emp where department_id not in(select dept_id from dept);

select emp.* from emp e
left join department d
on e.emp_id=d.dept_id
where dept_name is null

--second highest salary in each department
select * from (select e.*, dense_Rank() over(partition by dept_id order by salary desc) as rn from emp) a
where rn=2;

#break this query into two parts first subquery and then main_query)

--find all transactions done by shilpa
select * from orders where UPPER(name)='SHILPA'

--update query to swap gender


--pivot rows to column

--original table contains columns emp_id,salary_component,val
--important to learn sum function with cast when

select emp_id,
sum(case when salary_component='salary' then val end) as salary,
sum(case when salary_component='bonus' then val end) as bonus,
sum(case when salary_component='hike_percent' then val end) as hike_percent
--to create table till above part Use
--into emp_com_table
--this will create table name 'emp_com_table'
from emp_compensation
group by emp_id;

--unpivot column to rows

--using same table as previous
select emp_id,'salary' as salary_component, salary as val from emp_com_table
union all
select emp_id,'bonus' as salary_component, bonus as val from emp_com_table
union all
select emp_id,'hike_percent' as salary_component, hike_percent as val from emp_com_table


--output of all joins where values are duplicates

table1			table2
ID				ID
1				1
1				1
				1
			
inner join,left join, right join,full outer join will have same number of records
select * from table1 b inner join table2 a on b.ID=a.ID
o/p : 6 records all have value 1 in it

table1			table2
ID				ID
1				1
1				1
2				1
				3

here inner join will still give 6 records as output
right join will give 6 records, 6 will have 1 as value and other with 3 - NULL as it has no matched value
while left join will give 7 records, 6 will have 1 as value and other with 2 - NULL as it has not matched value
and full outer join will have 8 records , with 2 - NULL and 3 - NULL.

--find mode in sql table

--method-1 find mode with cte
with freq as(
select ID, count(*) as freq_cnt from `cricket_dataset.demo3` group by ID)
select * from freq where freq_cnt=(select max(freq_cnt) from freq)

--method-2 with rank()
--here we have to use two cte one for duplicates count and other for ranking them.
with freq as(
select ID, count(*) as freq_cnt from `cricket_dataset.demo3` group by ID),
rank_cte as (select *, rank() over(order by ID desc) as rn from freq)
select * from rank_cte where rn=1


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
department wise higher salary using rank(),dense_rank(),row_number()

--first know what each function will do
select * , rank() over(order by salary desc )from employee; --gives rank 1 to highest salary
select * , dense_rank() over(order by salary desc )from employee; --gives rank 1 highest salary without skipping numbers
select * , row_number() over(order by salary desc )from employee; --gives numbering to each records

employee with highest salary in departments

------------------------------------------------------------------------------------------------------------------------------------------------
Custom Sort examples

--select India at top in world happines index consisting of all countries.
select * from(
select *, case when country='India' then 1 else 0 end as rnk
from happiness_index
) as a
order by rnk desc,happiness_index21 desc;

--another without subquery
select * from happiness_index
order by case when country='India' then 1 else 0 end as rnk desc,
happiness_index21 desc;

--------------------------------------------------------------------------------------------------------------------------------------------------
--find how many products fall into customer budget along with list of products
--in case of clash, chose the less costly product
create table `cricket_dataset.products`
(
product_id string,
cost int64
);

insert into `cricket_dataset.products` values ('P1',200),('P2',300),('P3',500),('P4',800);

create table `cricket_dataset.customer_budget`
(
customer_id int64,
budget int64
);

insert into `cricket_dataset.customer_budget` values (100,400),(200,800),(300,1500);

select * from `cricket_dataset.customer_budget`;

select * from `cricket_dataset.products`;

WITH running_cte as(
  select *, sum(cost) over(order by cost asc) as r_cost
  from  `cricket_dataset.products`    --calculate running cost
)
select customer_id,budget,count(1) as no_of_products,string_agg(product_id,',') as list_of_products from `cricket_dataset.customer_budget` cb   --join that running cost with budget table
left join running_cte r
on r.r_cost < cb.budget
group by customer_id,budget

------------------------------------------------------------------------------------------------------------------------------------------------
--for each region find house which has won maximum no of battles. display region, house and no of wins
CREATE TABLE cricket_dataset.king (
    k_no int64,
    king string,
    house string
);

-- Create the 'battle' table
CREATE TABLE cricket_dataset.battle (
    battle_number INT64,
    name string,
    attacker_king INT64,
    defender_king INT64,
    attacker_outcome INT64,
    region string,
);

INSERT INTO cricket_dataset.king (k_no, king, house) VALUES
(1, 'Robb Stark', 'House Stark'),
(2, 'Joffrey Baratheon', 'House Lannister'),
(3, 'Stannis Baratheon', 'House Baratheon'),
(4, 'Balon Greyjoy', 'House Greyjoy'),
(5, 'Mace Tyrell', 'House Tyrell'),
(6, 'Doran Martell', 'House Martell');

-- Insert data into the 'battle' table
INSERT INTO cricket_dataset.battle (battle_number, name, attacker_king, defender_king, attacker_outcome, region) VALUES
(1, 'Battle of Oxcross', 1, 2, 1, 'The North'),
(2, 'Battle of Blackwater', 3, 4, 0, 'The North'),
(3, 'Battle of the Fords', 1, 5, 1, 'The Reach'),
(4, 'Battle of the Green Fork', 2, 6, 0, 'The Reach'),
(5, 'Battle of the Ruby Ford', 1, 3, 1, 'The Riverlands'),
(6, 'Battle of the Golden Tooth', 2, 1, 0, 'The North'),
(7, 'Battle of Riverrun', 3, 4, 1, 'The Riverlands'),
(8, 'Battle of Riverrun', 1, 3, 0, 'The Riverlands');

--for each region find house which has won maximum no of battles. display region, house and no of wins
select * from cricket_dataset.battle;
select * from cricket_dataset.king;

--solution
with cte as(
select attacker_king as king,region from cricket_dataset.battle where attacker_outcome=1
union all
select defender_king,region from cricket_dataset.battle where attacker_outcome=0
) --this cte will gives us all the kings who have won

--here we will join it with kings table to get house_name and count wins by houses with help of inner join
--rank will help us in get max wins of houses
select * from(
select a.region,b.house,count(*) as no_of_wins, rank() over(partition by a.region order by count(*) desc) as rn
from cte a
join cricket_dataset.king b
on a.king = b.k_no
group by a.region,b.house
) a
where rn=1


--solution-2

select * from(
select a.region,b.house,count(*) as no_of_wins, rank() over(partition by a.region order by count(*) desc) as rn
from cricket_dataset.battle a
join cricket_dataset.king b                                                             --this part will give us house_name that have won
on b.k_no = case when attacker_outcome=1 then attacker_king else defender_king end      -- same as above explanation, rest of is same
group by a.region,b.house
) a
where rn=1
----------------------------------------------------------------------------------------------------------------------------------------------
--count of number of occurenece of character or word in a string
create table cricket_dataset.strings (name string);
delete from cricket_dataset.strings;
insert into cricket_dataset.strings values ('Ankit Bansal'),('Ram Kumar Verma'),('Akshay Kumar Ak k'),('Rahul');

--count number of spaces in string
select replace(name,' ','') as rep_name,name,    --replace space with nothing
length(name)-length(replace(name,' ','')) as cnt  --take count of space in string
from cricket_dataset.strings;

--take count of word 'Ra' in each string
select cnt from(
select replace(name,'Ra','') as rep_name,name,    --replace space with nothing
(length(name)-length(replace(name,'Ra','')))/length('Ra') as cnt  --take count of word in string divide by length of word we are finding count for
from cricket_dataset.strings) a;

select replace(name,'Ra','') as rep_name,name,    --replace space with nothing
(length(name)-length(replace(name,'Ra','')))/length('Ra') as cnt  --take count of word in string divide by length of word we are finding count for
from cricket_dataset.strings;
------------------------------------------------------------------------------------------------------------------------------------------------
--full outer join using union

Create Table cricket_dataset.Customer (Customer_id int64, customer_name string);
Create Table cricket_dataset.Customer_orders (Customer_id int64, orderDate date);

drop table cricket_dataset.Customer

Insert into cricket_dataset.Customer Values (1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E');

Insert into cricket_dataset.Customer_orders Values (1,'2022-01-05'),(2,'2022-01-06'),(3,'2022-01-07'),(4,'2022-01-08'),(6,'2022-01-09');

Select C.*,CO.*
From cricket_dataset.Customer C LEFT JOIN cricket_dataset.Customer_orders CO
ON C.Customer_id = co.Customer_id
UNION ALL
Select C.*,CO.*
From cricket_dataset.Customer C RIGHT JOIN cricket_dataset.Customer_orders CO
ON C.Customer_id = co.Customer_id
WHERE c.Customer_id IS NULL

---------------------------------------------------------------------------------------------------------------------------------------------------
--find business day between create and resolved date excluding weeknds and holiday

create table cricket_dataset.tickets
(
ticket_id string,
create_date date,
resolved_date date
);

insert into cricket_dataset.tickets values('1','2022-08-01','2022-08-03'),('2','2022-08-01','2022-08-12'),('3','2022-08-01','2022-08-16');

create table cricket_dataset.holidays
(
holiday_date date
,reason string
);

insert into cricket_dataset.holidays values
('2022-08-11','Rakhi'),('2022-08-15','Independence day');

select * from cricket_dataset.tickets

select * from cricket_dataset.holidays

--approach for understanding first find actual days, then find weeks in between two dates and then find actual business_days except holidays
--now left join and use between condition and group by inside subquery

select *, date_diff(resolved_date,create_date,day) as actual_Days,
date_diff(resolved_date,create_date,day) - 2*date_diff(resolved_date,create_date,week) as week_diff,
from cricket_dataset.tickets

--solution
select *, date_diff(resolved_date,create_date,day) as actual_Days,
date_diff(resolved_date,create_date,day) - 2*date_diff(resolved_date,create_date,week) - count_of_holiday as working_days from(
select ticket_id,create_date,resolved_date,count(holiday_date) as count_of_holiday, from `cricket_dataset.tickets`
left join `cricket_dataset.holidays`
on holiday_date between create_date and resolved_date
group by 1,2,3)

------------------------------------------------------------------------------------------------------------------------------------------------------
--Most Popular Room Types
--output the rommtypes along with the number of searches for the type
--if the filter for room_type has more than one romm type then consider each unique type as separate rows
--sort the result basis on no of searches in desc order

create table cricket_dataset.airbnb_searches 
(
user_id int64,
date_searched date,
filter_room_types string
);
insert into cricket_dataset.airbnb_searches values (1,'2022-01-01','entire home,private room'),(2,'2022-01-02','entire home,shared room'),(3,'2022-01-02','private room,shared room'),(4,'2022-01-03','private room');

select * from cricket_dataset.airbnb_searches

--approach is use string_split function in sql
--mssql
select value as room_type , count(1) as no_of_searches, 
cross apply string_agg(filter_room_types,',') as room_type 
from `cricket_dataset.airbnb_searches`
group by value
order by no_of_searches desc;

--non mssql
with room as
(select sum(case when filter_room_types like '%entire%' then 1 else 0 end) as en,
sum(case when filter_room_types like '%private%' then 1 else 0 end) as pr,
sum(case when filter_room_types like '%shared%' then 1 else 0 end) as sh
from `cricket_dataset.airbnb_searches`)
select 'entire home' as  value,en cnt from room
union all
select 'private room' as value,pr cnt from room
union all
select 'shared room' as value,sh cnt from room
order by cnt desc;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
--7 interview questions in 60min

CREATE TABLE cricket_dataset.users (
    USER_ID INT64,
    USER_NAME string NOT NULL,
    USER_STATUS string NOT NULL
);

CREATE TABLE cricket_dataset.logins (
    USER_ID INT64,
    LOGIN_TIMESTAMP DATETIME NOT NULL,
    SESSION_ID INT64,
    SESSION_SCORE INT64
);

select * from cricket_dataset.users

INSERT INTO cricket_dataset.users VALUES (1, 'Alice', 'Active');
INSERT INTO cricket_dataset.users VALUES (2, 'Bob', 'Inactive');
INSERT INTO cricket_dataset.users VALUES (3, 'Charlie', 'Active');
INSERT INTO cricket_dataset.users  VALUES (4, 'David', 'Active');
INSERT INTO cricket_dataset.users  VALUES (5, 'Eve', 'Inactive');
INSERT INTO cricket_dataset.users VALUES (6, 'Frank', 'Active');
INSERT INTO cricket_dataset.users VALUES (7, 'Grace', 'Inactive');
INSERT INTO cricket_dataset.users VALUES (8, 'Heidi', 'Active');
INSERT INTO cricket_dataset.users VALUES (9, 'Ivan', 'Inactive');
INSERT INTO cricket_dataset.users VALUES (10, 'Judy', 'Active');


INSERT INTO cricket_dataset.logins VALUES (1, '2023-07-15 09:30:00', 1001, 85);
INSERT INTO cricket_dataset.logins VALUES (2, '2023-07-22 10:00:00', 1002, 90);
INSERT INTO cricket_dataset.logins VALUES (3, '2023-08-10 11:15:00', 1003, 75);
INSERT INTO cricket_dataset.logins VALUES (4, '2023-08-20 14:00:00', 1004, 88);
INSERT INTO cricket_dataset.logins VALUES (5, '2023-09-05 16:45:00', 1005, 82);
INSERT INTO cricket_dataset.logins VALUES (6, '2023-10-12 08:30:00', 1006, 77);
INSERT INTO cricket_dataset.logins VALUES (7, '2023-11-18 09:00:00', 1007, 81);
INSERT INTO cricket_dataset.logins VALUES (8, '2023-12-01 10:30:00', 1008, 84);
INSERT INTO cricket_dataset.logins VALUES (9, '2023-12-15 13:15:00', 1009, 79);

drop table cricket_dataset.logins

INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (1, '2024-01-10 07:45:00', 1011, 86);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (2, '2024-01-25 09:30:00', 1012, 89);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (3, '2024-02-05 11:00:00', 1013, 78);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (4, '2024-03-01 14:30:00', 1014, 91);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (5, '2024-03-15 16:00:00', 1015, 83);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (6, '2024-04-12 08:00:00', 1016, 80);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (7, '2024-05-18 09:15:00', 1017, 82);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (8, '2024-05-28 10:45:00', 1018, 87);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (9, '2024-06-15 13:30:00', 1019, 76);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-25 15:00:00', 1010, 92);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-26 15:45:00', 1020, 93);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-27 15:00:00', 1021, 92);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-28 15:45:00', 1022, 93);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (1, '2024-01-10 07:45:00', 1101, 86);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (3, '2024-01-25 09:30:00', 1102, 89);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (5, '2024-01-15 11:00:00', 1103, 78);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (2, '2023-11-10 07:45:00', 1201, 82);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (4, '2023-11-25 09:30:00', 1202, 84);
INSERT INTO cricket_dataset.logins (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (6, '2023-11-15 11:00:00', 1203, 80);


select * from cricket_dataset.users;

select * from cricket_dataset.logins;


--find the username which have not logged since 5 months
--approach is get the users who have logged before the current_date-5 months back
select USER_ID from cricket_dataset.logins
group by USER_ID
having max(LOGIN_TIMESTAMP) < date_add(CURRENT_DATE(), INTERVAL -5 MONTH)


-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--delete duplicate records, first identify and then delete
delete from employees where (emp_id,timestamp) in (select emp_id,min(timestamp) as timestamp from employees group by emp_id having count(1)>1);

--it can be based on what columns of your data suits best

--How to delete pure duplicates where you cannot specify the unique column to delete duplicate ones
create table emp_backup as select * from employee;
--then truncate the main table
--then do 
select distinct * from emp_backup
--insert the distinct values into main table
insert into employee select distinct * from emp_backup;

--another option is using row_number()
insert into employees (select *,row_number() over(partition by emp_id order by salary)as rn from emp_backup)

--for without backup approach
delete from employees where (emp_id,timestamp) not in (select emp_id,max(timestamp) as timestamp from employees group by emp_id);

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--first and last value functions
create table cricket_dataset.employee(
    emp_id int64,
    emp_name string,
    dept_id int64,
    salary int64,
    manager_id int64,
    emp_age int64
);

insert into cricket_dataset.employee values(1,'Ankit',100,10000,4,39);
insert into cricket_dataset.employee values(2,'Mohit',100,15000,5,48);
insert into cricket_dataset.employee values(3,'Vikas',100,10000,4,37);
insert into cricket_dataset.employee values(4,'Rohit',100,5000,2,16);
insert into cricket_dataset.employee values(5,'Mudit',200,12000,6,55);
insert into cricket_dataset.employee values(6,'Agam',200,12000,2,14);
insert into cricket_dataset.employee values(7,'Sanjay',200,9000,2,13);
insert into cricket_dataset.employee values(8,'Ashish',200,5000,2,12);
insert into cricket_dataset.employee values(9,'Mukesh',300,6000,6,51);
insert into cricket_dataset.employee values(10,'Rakesh',500,7000,6,50);


select * from cricket_dataset.employee;

--first value()
select *,first_value(emp_name) over(partition by dept_id order by emp_age) as youngest_employee from cricket_dataset.employee

--last_value()
select *,last_value(emp_name) over(partition by dept_id order by emp_age) as youngest_employee from cricket_dataset.employee
--using last_value without range clause treats each row as oldest, hence we have to use range in case of last value
select *,last_value(emp_name) over(order by emp_age rows between current row and unbounded following) as youngest_employee from cricket_dataset.employee
--another way by reversing the sorting
select *,first_value(emp_name) over(partition by dept_id order by emp_age desc) as youngest_employee from cricket_dataset.employee
-------------------------------------------------------------------------------------------------------------------------------
--PWC interview problem, if name is same exclude that record from output

create table cricket_dataset.source(id int64, name string);

create table cricket_dataset.target(id int64, name string);

insert into cricket_dataset.source values(1,'A'),(2,'B'),(3,'C'),(4,'D');

insert into cricket_dataset.target values(1,'A'),(2,'B'),(4,'X'),(5,'F');


select * from cricket_dataset.source
select * from cricket_dataset.target

--using full outer join
--appraoch is doing full join,filtering data on null and not null condition and then use case when to get the required columns and its values
select coalesce(s.id,t.id)as id,s.name,t.name,
case when t.name is null then 'new in source' when s.name is null then 'new in target'
else 'mismatch'
end as comment
from cricket_dataset.source s
full join cricket_dataset.target t on s.id=t.id
where s.name != t.name or s.name is null or t.name is null

--using union all
with cte as (
select *,'source' as table_name from cricket_dataset.source
union all
select *,'target' as table_name from cricket_dataset.target
)
select id,
case when min(name)!=max(name) then 'mismatch'
when min(table_name)='source' then 'new in source'
else 'new in target'
end as comment
from cte
group by id
having count(*)=1 or (count(*)=2 and min(name) != max(name))
--count(*),min(name)as min_name,max(name) as max_name,min(table_name) as table_name_min,max(table_name) as table_name_max,
-----------------------------------------------------------------------------------------------------------------------------
--Order of execution SQL

eg: select * from emp where='condition' order by 'condition' limit 5;

from-->first   second-->join    third-->where   -->group by    -->having    fourth-->select
fifth-->order by    sixth-->limit
------------------------------------------------------------------------------------------------------------------------------
--Top 5 advanced SQL interview problems

select * from `cricket_dataset.employee`

--que_type : top 3 products by sales, top 3 employees by salaries,within dept

--1. top 2 highest salaried employees
    --rn<2 meaning highest salary in each dept
    --in case of same salary, use dense_rank or ask interviewer
select * from(
select *,
row_number() over(partition by dept_id order by salary desc) as rn,
dense_rank() over(partition by dept_id order by salary desc) as rn_dense
from cricket_dataset.employee)a
where rn_dense<=2

--basic approach
select * from cricket_dataset.employee
order by dept_id,salary desc limit 2


--2. top 5 products by sales
with cte as(
  select product_id,sum(sales) as sales
  from orders
  group by product_id
)
select product_id,cte.sales
from cte
order by sales desc limit 5

--3. top 5 products by sales and cateory
with cte as(
  select category,product_id,sum(sales) as sales
  from orders
  group by category,product_id
)
select * from(
  select *,
  row_number() over(partition by category order by sales desc) as rn
  from cte
)a
where rn<=5
from cte
-------------------------------------------------------------------------------------------------------------------------
