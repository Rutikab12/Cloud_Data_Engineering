--LeetCode Problems: 175
--Combine Two TABLES
select p.firstName,p.lastName,a.city,a.state from Person p
left join Address a
on p.personId=a.personId

------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 176
--Find second highest salary

Select MAX(Salary) as SecondHighestSalary from Employee
where Salary < (Select MAX(Salary) from Employee)
-------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 177
--Find Nth Highest salary

--1st solution
CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
BEGIN
  RETURN (
      # Write your MySQL query statement below.
    select Salary as getNthHighestSalary from(
        select *,
        dense_rank() over(order by salary desc) as rn
        from Employee) as subq
        where rn=N
        limit 1
    );
END

--2nd solution using offset
CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
BEGIN
SET N = N-1;
  RETURN (
      SELECT DISTINCT(salary) from Employee order by salary DESC
      LIMIT 1 OFFSET N
      
  );
END
-----------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 178
--Rank Scores in desc

--using dense_rank()
with cte as(
select score,
dense_rank() over(order by score desc) as rn
from Scores)
select score,rn as 'rank' from cte
------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 181
--Employees earning more than their managers

--using self join
select e1.name as Employee from employee e1
join employee e2
where e2.id=e1.managerId and e1.salary > e2.salary

--2nd solution
select e1.name as Employee from employee e1
join employee e2
on e2.id=e1.managerId where e1.salary > e2.salary
---------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 182
--Remove Duplicate Email

--1st solution
select email as Email from Person
group by email having count(*)>1;

--2nd solution
select email
from person 
group by email
having count(email)>1
--------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 603
--Consecutive Available Seats (Premium)

--use lead() and lag() for getting consecutive seats
select seat_id from(
select *,lead(free) over(order by seat_id asc) as next_seat,lag(free) over(order by seat_id asc) as prev_seat
from vfscmuat_dh_lake_comms_ie_dev_staging_s.cinema) as sub
where free=1 and next_seat=1
or free=1 and prev_seat=1
order by seat_id;
---------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 185
--185. Department Top Three Salaries

--use dense_rank() with cte or subquery
with cte as(
select d.name as Department,e.name as Employee,e.salary as Salary,
dense_rank() over(partition by e.departmentId order by salary desc) as drn
from employee e
join department d
on e.departmentId=d.id)
select Department,Employee,Salary from cte where drn<=3;
--------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 184
--184. Department Highest Salary

--i will use dense_rank() and cte
--The difference between RANK() and DENSE_RANK() 
--is that RANK() will create gaps in rank values when there are ties, 
--whereas DENSE_RANK() will not.
with cte as(
select d.name as Department,e.name as Employee,e.salary as Salary,
rank() over(partition by e.departmentId order by salary desc) as drn
from employee e
join department d
on e.departmentId=d.id)
select Department,Employee,Salary from cte where drn=1;
-----------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 183
--183. Customers Who Never Order

--use not in operator
select name as 'Customers'
from Customers
where id not in (select customerId from orders);
--------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 196
--196. Delete Duplicate Emails

-- delete from person
-- where id not in(select min(id) from person group by email);
DELETE p 
FROM person p
INNER JOIN person p2 ON p.email = p2.email
WHERE p.id > p2.id;
----------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 577
--577. Employee Bonus

--use left join mainly and to eliminate >1000 reords use bonus is null condition.
select e.name,b.bonus from employee e
left join bonus b
on e.empId=b.empId
where b.bonus<1000 or b.bonus is null
----------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 585
--585. Investments in 2016

--did not understand
with cte as(
select tiv_2016,
count(*) over(partition by tiv_2015) as tiv_2015_cnt,
count(*) over(partition by lat,lon) as lat_lan
from Insurance)
select round(sum(tiv_2016),2) as tiv_2016
from cte
WHERE tiv_2015_cnt > 1
AND lat_lan = 1;

----------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems :586
--586 : Customer Placing largest number of orders

--using cte and selecting max cause if duplicate count comes, it should not be missed.
with cte as(
select customer_number,count(order_number) as cnt from Orders
group by customer_number)
select customer_number from cte
where cnt=(select max(cnt) from cte);

-----------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 595
--595 : Big Countries
--just use where condition
select name , population , area from world where area >= 3000000 or population >= 25000000;

------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 596
--596 :Classes with more than 5 students

--use group by and count(class)
select class from courses group by class having count(class)>=5;

-------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems :597
--597 : Friend Requests overall acceptance rate

select * from cricket_dataset.request_accepted;

select * from cricket_dataset.friend_request;

--Write a query to find the overall acceptance rate of requests rounded to 2 decimals, which is the number of acceptance divide the number of requests.

/*Note:
The accepted requests are not necessarily from the table friend_request. In this case, you just need to simply count the total accepted requests (no matter whether they are in the original requests), and divide it by the number of requests to get the acceptance rate.
It is possible that a sender sends multiple requests to the same receiver, and a request could be accepted more than once. In this case, the ‘duplicated’ requests or acceptances are only counted once.
If there is no requests at all, you should return 0.00 as the accept_rate.*/


SELECT 
ROUND(IFNULL(
  (SELECT COUNT(DISTINCT requester_id, accepter_id) from cricket_dataset.request_accepted) / (SELECT COUNT(DISTINCT sender_id, send_to_id) from cricket_dataset.friend_request)
  ,0),2) AS accept_rate;
  
/*
accept_rate|
------------
0.80	   |
*/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 610
--610 : Triangle Judgement

--use case when with x+y=z and y+z=x and z+x=y then yes else no
SELECT x, y, z,
       CASE
           WHEN x + y > z AND x + z > y AND y + z > x THEN 'Yes'
           ELSE 'No'
       END AS triangle
FROM Triangle;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 613
--Shortest distance in a line

--appraoch is to use self join

create table cricket_dataset.point(
  x float64
);

insert into cricket_dataset.point values(-1),(0),(2);

select * from cricket_dataset.point order by x;

--We can use a self-join to join each point in the table with the larger points, and then calculate the distance between the two points. Finally, we can take the minimum distance.

select min(p2.x-p1.x) as shortest from cricket_dataset.point p1
join cricket_dataset.point p2
on p1.x > p2.x

/*
+----------+
| shortest |
+----------+
| 1        |
+----------+
*/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1445
--1445 :Apple and Oranges

/*
find the differenc between apple and oranges sold each day
*/

--use case when and take diff
with cte as(
select sale_date,
sum(case when fruit='apples' then sold_num else 0 end) as apple_count,
sum(case when fruit='oranges' then sold_num else 0 end) as orange_count 
from cricket_dataset.sales
group by sale_date)
select sale_date,(apple_count-orange_count) as diff from cte

--use group by and sum
select sale_date,sum(if(fruit='apples',sold_num,-sold_num)) as diff
from cricket_dataset.sales
group by 1
order by 1;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 619
--619 : Biggest Single number

--use dupliate logic and takeout max(num)
select max(num) as num from (select num from mynumbers group by num having count(num)=1) as a;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 620
--620 :Boring Movies

--use id%2 != 0 as logic for odd numbers
select id,movie,description,rating from cinema
where description != 'boring' and id%2<>0
order by rating desc;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems :627
--627 : Swap Salary

--pdate salary set sex= case when sex='f' then 'm' else 'f' end
UPDATE salary
SET sex = if(sex = 'm', 'f', 'm')
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems :2084
--2084 : Drop Type 1 Orders for Customers With Type 0 Orders

--choose min(order_type) and partition it by customer_id and then compare order_type=min_order_type 

with cte as(
select *,
min(order_type) over(partition by customer_id order by customer_id) as min_order_type
from cricket_dataset.orders)
select order_id,customer_id,order_type from cte where order_type = min_order_type
order by order_type desc

--or in where condition of cte, we can use order_type+min_order_type<>1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1393
--1393 :Capital gain/loss

--with cte and row_number()
with cte as(
select *,
row_number() over(partition by stock_name,operation) as stock_history
from stocks)
select stock_name,sum(case when operation='Buy' then -price else price end)as capital_gain_loss from cte
group by stock_name;

--without cte , using only case when and group by
select stock_name,sum(case when operation='Buy' then -price else price end)as capital_gain_loss from stocks
group by stock_name
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems :1783
--1783 : Grand Slam Titles

--Write a solution to report the number of grand slam tournaments won by each player. Do not include the players who did not win any tournament.
--first do the pivot,ie change championship table (pivot) and perform union all and group by player_id with count
--then do left join on cte and player table

with cte1 as(
select year,'Wimbledon' as Championship ,Wimbledon as player_id from cricket_dataset.championships
union all
select year,'Fr_open' as Championship ,Fr_open as player_id from cricket_dataset.championships
union all
select year,'US_open' as Championship ,US_open as player_id from cricket_dataset.championships
union all
select year,'Au_open' as Championship ,Au_open as player_id from cricket_dataset.championships),
cte2 as(
  select player_id,count(player_id) as grand_slams_count from cte1 group by player_id
)
select c.player_id,p.player_name,c.grand_slams_count from cte2 c
left join cricket_dataset.players p
on p.player_id=c.player_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1050
--1050 : Actors and Directors who incorporate more than 3 times

--do group by and take count of timestamp
select actor_id,director_id from actordirector
group by actor_id,director_id
having count(timestamp)>=3;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1113
--1113 : Reported Posts

--use date,action and extra is not null in where condition and take count of disticnt post_id

select extra as reason_for,count(distinct post_id) as no_of_posts from cricket_dataset.actions
where action='report' and action_date = '2019-07-04' and extra is not null
group by 1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1211
--1211 - Queries Quality and Percentage

--use sum() and avg()
SELECT
    query_name,
    ROUND(AVG(rating / position), 2) AS quality,
    ROUND(AVG(rating < 3) * 100, 2) AS poor_query_percentage
FROM Queries
where query_name is not null
GROUP BY 1;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1241
--1241 - Number of Comments per Post

--do self join and then take count of sub_id, group by and order by post_id

WITH t AS (
    SELECT DISTINCT s1.sub_id AS post_id, s2.sub_id AS sub_id
    FROM cricket_dataset.submissions AS s1
    LEFT JOIN cricket_dataset.submissions AS s2 ON s1.sub_id = s2.parent_id
    WHERE s1.parent_id IS NULL
)
SELECT post_id, COUNT(sub_id) AS number_of_comments
FROM t
GROUP BY post_id
ORDER BY post_id;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1241
--1251 - Average Selling price

--use group by and COALESCE
select p.product_id,COALESCE(round(sum(p.price*u.units)/sum(u.units),2),0) as average_price from prices p
left join unitssold u
on p.product_id=u.product_id and purchase_date between start_date and end_date
group by product_id
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1241
--1294 - Weather Type in Each Country

--use case when and join country_id and extract month , group by country_name
select c.country_name,
case when avg(w.weather_state)<=15 then 'Cold'
when avg(w.weather_state)>=25 then 'Hot' else
'Warm' end as weather_type
from `cricket_dataset.countries` c
join `cricket_dataset.weather` w
on c.country_id=w.country_id
WHERE EXTRACT(MONTH from day) = 11
GROUP BY c.country_name ;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1303
-- 1303. Find the Team Size

--use window function and do partitions based on team_id
select employee_id,
count(employee_id) over(partition by team_id order by team_id) as team_size
from cricket_dataset.employee
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems : 1280
-- 1280 : Students and examinations

--use left join
select s.student_id,s.student_name,sn.subject_name,count(e.subject_name) as attended_Exams from students s
join subjects sn left join examinations e
on s.student_id=e.student_id and e.subject_name=sn.subject_name
group by s.student_id,sn.subject_name,s.student_name
order by s.student_id,sn.subject_name
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1285
-- 1285 : Find the Start and End Number of Continuous Ranges

--count the diffeernce by giving row_number, if 0 range present else not
--then find min mand max of number from cte
with cte as(
select log_id,log_id-row_number() over(order by log_id) as diff
from cricket_dataset.logs)
select min(log_id) as start_id , max(log_id) as end_id
from cte
group by diff
order by start_id
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1699
-- 1699 : Number of Calls Between Two Persons

--use case when to get person_1 and 2
--then wrap it up in cte and do groupby 1,2 and sum(duration) and count()all
with cte as(
select *, case when from_id<to_id then from_id else to_id end as person_1,
case when from_id<to_id then to_id else from_id end as person_2
from cricket_dataset.calls)
select person_1,person_2,sum(duration) as duration,count(*) as call_counts,
from cte
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2066
-- 2066 : Account Balance

--use normal case when and sum and then partition it by ccount_id
select account_id,day,
sum(case when type='Deposit' then amount else -amount end) over(partition by account_id order by account_id,day) as balance
from cricket_dataset.transactions
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1378
-- 1378 : Replace employee_id with unique identifier

select a.unique_id,b.name from employeeUNI a
right join employees b
on a.id=b.id

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1350
-- 1350 - Students With Invalid Departments

--use not in operator
select s.id,s.name from cricket_dataset.students s
where department_id not in (select id from cricket_dataset.departments);
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----LeetCode Problems: 1407
--1407 : Top Traveller

--use left join and COALESCE with sum(distance)
select u.name,coalesce(sum(r.distance),0) as travelled_distance
from users u
left join rides r
on u.id=r.user_id
group by u.id,u.name
order by travelled_distance desc,name asc

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1421
--1421 :NPV Queries

--do left join on queries
select q.id,q.year,coalesce(n.npv,0) as npv from cricket_dataset.queries q
left join cricket_dataset.npv n
on n.id=q.id and n.year=q.year
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1435
--1435 : Create a Session Bar Chart

--we can use between instead greater or equal operator
SELECT '[0-5>' AS bin, COUNT(1) AS total FROM cricket_dataset.sessions WHERE duration < 300
UNION ALL
SELECT '[5-10>' AS bin, COUNT(1) AS total FROM cricket_dataset.sessions WHERE 300 <= duration AND duration < 600
UNION ALL
SELECT '[10-15>' AS bin, COUNT(1) AS total FROM cricket_dataset.sessions WHERE 600 <= duration AND duration < 900
UNION ALL
SELECT '15 or more' AS bin, COUNT(1) AS total FROM cricket_dataset.sessions WHERE 900 <= duration;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1484
--1484 - Group Sold Products By The Date

--use group concat and count distinct product
SELECT sell_date,COUNT(DISTINCT product) AS num_sold,
GROUP_CONCAT(distinct product order by product) AS products
FROM Activities GROUP BY sell_date
ORDER BY sell_date;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1565
--1565 :Unique Orders and Customers Per Month

--use substring and count distinct customers and order_id
select substring(cast(order_date as string),0,7) as month,count(distinct customer_id) as customer_count,count(order_id) as order_count from cricket_dataset.orders
where invoice>20
group by substring(cast(order_date as string),0,7)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1596
--1596 :The Most Frequently Ordered Products for Each Customer

--first take count of customer_id;s and rank them
--then select only those whose rk=1
--and join cte with product_table
with cte1 as(
select customer_id,product_id ,rank() over(partition by customer_id order by count(*) desc) as rk
from cricket_dataset.orders
group by customer_id,product_id),
cte2 as(
select * from cte1 where rk=1)
select c.customer_id,p.product_id,p.product_name from cte2 c
join `cricket_dataset.products` p
on c.product_id=p.product_id

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1204
--1204 :Last Person to fit in bus

--use cte with sum for running sum
with cte as(
select *,sum(weight) over(order by turn) as Total_Weight
from queue)
select person_name from cte where Total_weight <=1000
order by turn desc limit 1;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 1907
--1907 :Count Salary Categories

--use union all and sum aggregate function
select "Low Salary" as Category,sum(case when income<20000 then 1 else 0 end) as accounts_count
from accounts
union
select "Average Salary" as Category,sum(case when income between 20000 and 50000 then 1 else 0 end) as accounts_count
from accounts
union
select "High Salary" as Category,sum(case when income>50000 then 1 else 0 end) as accounts_count
from accounts

--2nd solution,using cte and then union all
with cte as (
    SELECT 
    SUM(income < 20000) as `Low`,
    SUM(20000 <= income && income <= 50000) as `Avg`,
    SUM(50000 < income) as `High`
    FROM Accounts
)
SELECT 'Low Salary' AS category, `Low` AS accounts_count FROM cte
UNION ALL
SELECT 'Average Salary' AS category, `Avg` AS accounts_count FROM cte
UNION ALL
SELECT 'High Salary' AS category, `High` AS accounts_count FROM cte
ORDER BY accounts_count;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem: 602
--602 :Friend Request II - Who has most friends

--use subquery as with cte it is not accepting solution
SELECT id, COUNT(*) AS num 
FROM (SELECT requester_id AS id FROM RequestAccepted
UNION ALL
SELECT accepter_id FROM RequestAccepted
) AS friends_count
GROUP BY id
ORDER BY num DESC 
LIMIT 1;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1978
--1978 :Employee who manager left the company

--using subquery and left join
select employee_id from(
 select e1.* from employees e1
 left join employees e2
 on e1.manager_id=e2.employee_id
 where e1.manager_id is not null and e2.employee_id is null
) as em
where salary<30000
order by employee_id

--2nd solution using not in clause
select employee_id
from employees
where salary < 30000 and 
manager_id not in (
        select employee_id from employees
    )
order by employee_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1571 
--1571 : Warehouse Manager

--use inner join and then sum it up as volume and group by warehouse name
select w.name as warehouse_name,sum(w.units*(p.Length*p.Height*p.Width)) as volumne
 from cricket_dataset.products p
join cricket_dataset.warehouse w
on p.product_id=w.product_id
group by w.name

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1581
--1581. Customer Who Visited but Did Not Make Any Transactions

# Write your MySQL query statement below
select a.customer_id,count(a.visit_id) as count_no_trans from visits a
left join transactions b
on a.visit_id=b.visit_id
where b.transaction_id is null
group by a.customer_id

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1587
--1587 : Bank Account Summary II

--use group by account and having amount>10000
select u.name,sum(t.amount) as balance from Users u
join Transactions t
on u.account=t.account
GROUP BY t.account
HAVING balance > 10000;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1683
--1683 : Invalid Tweets

select tweet_id from tweets where length(content) > 15;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1693
--1693. Daily Leads and Partners

--use count distincts
select date_id,make_name,count(distinct lead_id) as unique_leads,count(distinct partner_id) as unique_partners 
from dailysales
group by date_id,make_name;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1581
--1581. Maximum transactions each day

--use window functions and rank it
with cte as(
select transaction_id,rank() over(partition by cast (day as date) ORDER BY amount DESC) AS rk
from cricket_dataset.transactions)
select transaction_id from cte
where rk=1
order by 1;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1741
--1741. Total Time Spend by Each employee

--use group by and sum of in_time and out_time
select event_day as day,emp_id,sum(out_time-in_time) as total_time
from employees
group by event_day,emp_id;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1757
--1757. Recyclable and Low Fat Products

--
select product_id from products where low_fats='Y' and recyclable='Y';
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1821
--1821. Find Customers with Positive revenue this year

select * from cricket_dataset.customers
where year=2021 and revenue>0
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1873
--1873. Calculate Special bonus

-- use even odd logic and not like operator
with cte as(
select *, case when employee_id%2!=0 and name not like 'M%' then salary else 0 end as bonus
from employees)
select employee_id,bonus from cte order by employee_id;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1468
--1468. Calculate Salaries

--use max and patition by company_id
--then use case when as per conditions given
with cte as(
select *,max(salary) over(partition by company_id) as max_salary from `cricket_dataset.salaries`
)
select company_id,employee_id,employee_name,
ROUND(CASE WHEN max_salary < 1000 THEN salary
WHEN max_salary >= 1000 AND max_salary <= 10000 THEN salary * 0.76
ELSE salary * 0.51
END
) AS salary
from cte
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1965
--1965. Employees with Missing Information

--use union all and order by employee_id asc
select employee_id from employees where employee_id not in (select employee_id from salaries)
union
select employee_id from salaries where employee_id not in (select employee_id from employees)
order by 1

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1890
--1890. Latest Login in 2020

--use max + group by functino
select user_id,max(time_stamp) as last_stamp from logins
where Year(time_stamp)=2020
group by user_id;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2026
--2026. Low-Quality Problems

--use with clause and normal round function to calculate Percentage
with cte as(
select problem_id,round(likes/(likes+dislikes)*100,2) as like_per from `cricket_dataset.problems`)
select problem_id from cte where like_per < 60.00
order by 1
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2082
--2082. Number of Rich customers

--use distinct count
select count(distinct customer_id) as rich_count
from store
where amount > 500;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1795
--1795 : Rearrange PRoducts table

--use union to create rows to column (pivot)
select product_id,'store1' as store, store1 as price from products where store1 is not null
union
select product_id,'store2' as store, store2 as price from products where store2 is not null
union
select product_id,'store3' as store, store3 as price from products where store3 is not null

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 534
--534 - Game Play Analysis III

--window function using sum() over() and sorting by event_Date and then calculate running sum
select player_id,event_date,
sum(games_played rows between unbounded preceeding and current row) over(partition by player_id order by event_date) as total_games 
from cricket_dataset.activity
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1398
--1398 - Customers Who Bought Products A and B but Not C

--use sum(product_name='A')>0 in having as customers who bought product A,B,C , enclosed it within cte and then use IN operator
with cte as(
select customer_id
from cricket_dataset.orders
group by customer_id
having sum(product_name='A')>0 and sum(product_name='B')>0  and sum(product_name='C')=0
)
select * from cricket_dataset.customers
where customer_id in (select customer_id from cte)
order by 1

--another solution using left join+group by+having
SELECT customer_id, customer_name
FROM
    Customers
    LEFT JOIN Orders USING (customer_id)
GROUP BY 1
HAVING SUM(product_name = 'A') > 0 AND SUM(product_name = 'B') > 0 AND SUM(product_name = 'C') = 0
ORDER BY 1;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----LeetCode Problems: 1853
--1853 : Convert Date Format

--use mysql date time functions
SELECT DATE_FORMAT(day, '%W, %M %e, %Y') AS day FROM Days;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----LeetCode Problems: 1173
--1173 - Immediate Food Delivery I

--use with clause and take distinct attributes and then use round(sum()/count()) for Percentage
with cte as(
select distinct customer_id,order_date,customer_pref_delivery_date as ccpd
from cricket_dataset.delivery
order by customer_id,order_date)
select round(sum(case when order_date=cte.ccpd then 1 else 0 end)/count(*)*100,2) as immediate_percentage
from cte;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1069
-- 1069 : Product Sales Analysis II

--two solutions one using window functions and other by using just group by and sum()
select distinct product_id,sum(quantity) over(partition by product_id) as total_quantity from cricket_dataset.sales

select product_id,sum(quantity) as total_quantity from cricket_dataset.sales
group by product_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1068
-- 1068 : Product Sales Analysis I

--use inner join or only where condition where it treats that condition as join

select b.product_name,a.year,a.price from sales a
inner join product b
on a.product_id=b.product_id;

select Product.product_name , Sales.year, Sales.price from Product , Sales
where Product.product_id=Sales.product_id ;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 511
-- 511 : Game Play Analysis I

--use group by and min(date)
select a1.player_id,min(a1.event_date) as first_login from activity a1
group by 1;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1715
-- 1715 : Count Apple and oranges

--use case when for chest_id is null and take sum of it.
select sum(case when b.chest_id is null then b.apple_count else (b.apple_count+c.apple_count) end) as apple_count,
sum(case when b.chest_id is null then b.orange_count else (b.orange_count+c.orange_count) end) as orange_count
from cricket_dataset.boxes b
left join cricket_dataset.chests c
on b.chest_id=c.chest_id

--using if null or coalesce
select
sum(ifnull(b.apple_count, 0) + ifnull(c.apple_count, 0)) as apple_count,
sum(ifnull(b.orange_count, 0) + ifnull(c.orange_count, 0)) as orange_count
from cricket_dataset.boxes as b
left join cricket_dataset.chests as c 
on b.chest_id = c.chest_id;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1789
-- 1789 : Primary Department for each employee

--first using union 
SELECT employee_id, department_id
FROM Employee
WHERE primary_flag = 'Y'
UNION
SELECT employee_id, department_id
FROM Employee
GROUP BY employee_id
    HAVING COUNT(*) = 1;
	
--using IN operator
select e.employee_id,e.department_id from employee e
where e.primary_flag='Y' OR e.employee_id in (
    select employee_id from employee group by employee_id having count(employee_id)=1
)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1789
-- 1789 : Primary Department for each employee

--using left join+ group by +having
select p.product_name,sum(o.unit) as unit from products p
left join orders o
on p.product_id=o.product_id
and o.order_date between '2020-02-01' and '2020-02-29'
group by o.product_id
having unit>=100

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1148
-- 1148 : Article Views I

--use distinct,IN operator and order by id
select distinct author_id as id from views
where author_id in (select author_id from views where viewer_id=author_id) 
order by id asc
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1082
--1082 - Sales Analysis I

--use group by + having (subquery)
select s.seller_id from cricket_dataset.sales s
group by s.seller_id
having sum(price) = (select sum(price) as price from cricket_dataset.sales
                      group by seller_id 
                      order by price desc limit 1)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2041
--2041 - Accepted Candidates From the Interviews

--use left join+ where + group by + having clause
select c.candidate_id from cricket_dataset.candidates c
left join cricket_dataset.rounds r
on c.interview_id=r.interview_id
where c.years_of_exp>=2
group by c.candidate_id  --we can use interview_id also id didn't workout in BQ hence I used candidate_id
having sum(r.score)>=15
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1934
--1934 - Confirmation Rate

--use round(avg(case when 1 else 0)) + left join+ group by
select s.user_id,
round(avg(case when c.action='confirmed' then 1 else 0 end),2) as confirmation_rate
from signups s
left join confirmations c
on s.user_id=c.user_id
group by s.user_id
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1867
--1867 - Orders With Maximum Quantity Above Average

--use max + avg + group by in cte and then query and compare
with cte as(
select order_id,max(quantity) as max_quantity,round(avg(quantity),2) as avg_quantity from cricket_dataset.orders_details
group by order_id)
select order_id from cte
where cte.max_quantity > (select max(cte.avg_quantity) from cte)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1729
--1729 : Find Followers Count

--use group by
select user_id, count(follower_id) as followers_count 
from followers 
group by user_id 
order by user_id asc;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1511
--1511 : Customer Order Frequency

--use join + group by +having sum()
--we can use using also --using(customer_id) and using(product_id)
select c.customer_id,c.name from cricket_dataset.customers c
join cricket_dataset.orders o on c.customer_id = o.customer_id
join cricket_dataset.product p on p.product_id = o.product_id
where extract(Year from order_date)=2020
group by 1
having sum(if(MONTH(order_date) = 6, quantity * price, 0)) >= 100
and sum(if(MONTH(order_date) = 7, quantity * price, 0)) >= 100;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1633
--1633 - Percentage of Users Attended a Contest

--use group by and subquery
SELECT contest_id,ROUND(COUNT(1) * 100 / (SELECT COUNT(1) FROM Users), 2) AS percentage
FROM Register
GROUP BY contest_id
ORDER BY percentage DESC, contest_id;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 607
--607 - Sales Person

--first solution using left join+ group by +having
SELECT s.name
FROM
    SalesPerson AS s
    LEFT JOIN Orders USING (sales_id)
    LEFT JOIN Company AS c USING (com_id)
GROUP BY sales_id
HAVING IFNULL(SUM(c.name = 'RED'), 0) = 0;

--second solution using IN+subquery
select name from salesperson 
where sales_id not in (
select distinct sales_id from orders
where com_id = (select com_id from company where name = "RED")
)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1077
--1077 - Project Employees III

--use left join + window function and cte
with cte as(
select p.project_id,e.employee_id,e.experience_years, rank() over(partition by project_id order by e.experience_years desc) as rk from cricket_dataset.project p
left join cricket_dataset.employee e
on p.employee_id=e.employee_id)
select project_id,employee_id from cte
where rk=1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2238
--2238 - Number of Times a Driver Was a Passenger

--use cte and count(distinct()) subquery
with cte as (
select distinct driver_id from cricket_dataset.rides)
select t.driver_id, count(passenger_id) as cnt
from cte as t
left join cricket_dataset.rides as r on t.driver_id = r.passenger_id
group by 1;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2072
--2072 - The Winner University

--first take count of student_id from both tables and then put them in case when and compare
select case when (select count(student_id) from cricket_dataset.california where score>=90) > (select count(student_id) from cricket_dataset.newyork where score>=90) then 'California University'
when (select count(student_id) from cricket_dataset.newyork where score>=90) > (select count(student_id) from cricket_dataset.california where score>=90) then 'New York University'
else 'No Winner' end as winner

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1075
--1075 - Project Employees I

--use join + round(Avg(exp+years),2)+group by
select p.project_id,round(avg(e.experience_years),2) as average_years
from project p
join employee e
on p.employee_id=e.employee_id
group by project_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1355
--1355 - Activity Participants

--use normal group by + where (Select min() and max()) or use row_number
with cte as(
select name,activity,count(activity) as cnt_act from Friends
--row_number() over(order by count(activity) desc) as rn 
group by activity)
select activity from cte
where cnt_act > (SELECT MIN(cnt_act) FROM cte) AND cnt_act < (SELECT MAX(cnt_act) FROM cte);

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1709
--1709 - Biggest Window Between Visits

--use lead(visit_date,1,default_value) over() + take diff of next and prev date + select max(diff) at Last
with cte as(
select user_id,lead(visit_date,1,'2021-1-1') over(PARTITION by user_id order by visit_date) as max  
from UserVisits),
cte2 as(
  select user_id,(max-visit_date) as max from cte
)
select user_id,max(max) as biggest_window from cte2
group by user_id
order by user_id;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1204
--1204 - Last Person to Fit in the Bus

--use rolling sum + cte
with cte as(
select *,sum(weight) over(order by turn) as total_weight
from Queue)
select person_name from cte
where total_weight<=1000
order by total_weight DESC
limit 1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1112
--1112 - highest-grade-for-each-student

--use row_number() + over() or rank() + over()
with cte as(
select *,row_number() over(partition by student_id order by grade desc) as drn
from Enrollments)
select student_id,course_id,grade from cte
where drn=1

--using rank()
with cte as(
select *,rank() over(partition by student_id order by grade desc,course_id) as drn
from Enrollments)
select student_id,course_id,grade from cte
where drn=1
order by student_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 608
--608 - Tree Node

--use case when and subquery for inner nodes
select id,
case when p_id is null then 'Root'
when id in (select p_id from Tree) then 'Inner'
else 'Leaf' end as type
from Tree;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1321
--1321 - Restaurant Growth

--first calculate sum using group By as total
--then calculate running sum and average using above total
--then add 6 days to date and return the result
with cte as(
select visited_on,sum(amount) as total_amt from Customer
group by visited_on),
cte2 as(
select visited_on,sum(total_amt) over(order by visited_on ROWS 6 PRECEDING) as total_amount,
round(avg(total_amt) over(order by visited_on ROWS 6 PRECEDING),2) as average_amt 
from cte)
select * from cte2
where visited_on >= (select visited_on from cte2 order by visited_on limit 1) + 6
order by visited_on
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1532
--1532 - The Most Recent Three Orders

--using equi join + window function
with cte as(
select c.name,o.order_date,o.customer_id,o.order_id,row_number() over(partition by o.customer_id order by o.order_date desc) as drn
from Orders o
join Customers c
on o.customer_id=c.customer_id)
select name,customer_id,order_id,order_date from cte
where drn<=3
order by 1,2,4 desc

--another solution
with cte as(
select o.order_date,o.customer_id,o.order_id,row_number() over(partition by o.customer_id order by o.order_date desc) as drn
from Orders o)
select c.name,e.customer_id,e.order_id,e.order_date from Customers c
join cte e
on c.customer_id=e.customer_id
where e.drn<=3
order by 1,2,4 desc
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2112
--2112 - The Airport With the Most Traffic

--first find sum by dept_id,arrival_id and then union it and calc rolling sum , then at last select max(sum)
with cte as(
select departure_airport as aid,sum(flights_count) as total from Flights
group by 1),
cte2 as(
select arrival_airport as aid,sum(flights_count) as total from Flights
group by 1),
cte3 as(
select * from cte
union
select * from cte2),
cte4 as(
select aid,sum(total) as ftot from cte3
group by aid)
select aid as airport_id from cte4
where ftot =(SELECT max(ftot) from cte4)

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 626
--626 - Exchange Seats

--use lead and lag + case when 
with cte as(
select *,lead(id) over(order by id) as next,
lag(id) over(order by id) as prev from seat)
select case when ((id%2=1)AND next is not null) then next
when (id%2=0) then prev
else id end as id,student
from cte
order by id

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1164
--1164 - Product Price at a Given Date

--first rank by product_id and with where condition extract latest price
--now for values not null union it with cte 
with cte as(
select *,rank() over(partition by product_id order by change_date desc) as rn
from products
where change_date <='2019-08-16')
select product_id, new_price as price
from cte
where rn=1
union
select product_id, 10 as price
from products
where product_id not in (select product_id from cte)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1045
--1045 - Customers Who Bought All Products

--use left join + count distinct
select a.customer_id from customer a
left join product b
on a.product_key=b.product_key
group by a.customer_id
having count(distinct a.product_key)=(select count(product_key) from product)

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1193
--1193 - Monthly Transactions I

--group by date_format and country , use case when for counts of transactions
select date_format(trans_date,'%Y-%m') as month,country,count(id) as trans_count,
sum(case when state='approved' then 1 else 0 end) as approved_count,sum(amount) as trans_total_amount,
sum(case when state='approved' then amount else 0 end) as approved_total_amount
from transactions
group by date_format(trans_date,'%Y%m'),country

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1158
--1158. Market Analysis I

--first appraoch using cte + left join
with cte as(
select u.user_id,sum(case when year(order_date)=2019 then 1 else 0 end) as orders_in_2019 from users u
left join orders o
on o.buyer_id=u.user_id
group by u.user_id)
select c.user_id as buyer_id,u.join_date,c.orders_in_2019 from cte c
left join users u
on c.user_id=u.user_id
order by c.user_id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1174
--1174. Immediate Food Delivery II

--use avg() and IN operator where select min(order_date)
select round(avg(order_date = customer_pref_delivery_date)*100,2) as immediate_percentage
from delivery
where (customer_id,order_date) IN (select customer_id,min(order_date) from delivery
group by 1)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 178
--178. Rank Scores

--use dense_rank()
with cte as(
select score,
dense_rank() over(order by score desc) as rn
from Scores)
select score,rn as 'rank' from cte
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1126
--1126 - Active Businesses

--create fourth column avg_occ stored it in cte and then use group by business_id and having count(event_type)>1
with cte as(
select *,avg(occurrences) over(partition by event_type) as occ_avg from Events)
select business_id
from cte
where occurrences > occ_avg
group by business_id
having count(event_type)>1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 176
--176 - Second Highest Salary

--use rank or row_number
with cte as(
select Salary,
row_number() over(order by salary desc) as rn
from Employee)
select MAX(Salary) as SecondHighestSalary
from cte where rn=2

--use normal query
Select MAX(Salary) as SecondHighestSalary from Employee
where Salary < (Select MAX(Salary) from Employee)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1667
--1667 - Fix Names in Table

--appraoch is first make first letter upper and remainig letters as lower and then concat them
--upper(substring(name,1,1)) as first_letter,lower(substring(name,2,len(name)-1)) as last_letter
select user_id,concat(upper(substring(name,1,1)),lower(substring(name,2,length(name)-1))) as name from users order by user_id;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2175
--2175 - The Change in Global Rankings

--initial appraoch is first with row_number take out intial rank,
--then take out new rank by adding points+point_change
--and after that just cast those ranks as signed integer and substract to get rank diff

with cte as(
select t.team_id,t.name,t.points,p.points_change,
ROW_NUMBER() over(order by t.points desc,name asc) as initial_rank,
ROW_NUMBER() over(order by t.points+p.points_change desc) as new_rank
from TeamPoints t
left join PointsChange p
on t.team_id=p.team_id)

select team_id,name,(cast(initial_rank as SIGNED)-cast(new_rank as SIGNED)) as rank_diff
from cte
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1549
--1549 - The Most Recent Orders for Each Product

--first rank and partition by product_id and select records where rk=1
with cte as(
select o.order_id,o.order_date,p.product_name,p.product_id,
rank() over(partition by o.product_id order by o.order_date desc) as rn 
from Orders o
left JOIN Products p
on o.product_id=p.product_id
)
select p.product_id,p.product_name,o.order_id,o.order_date from cte
where rn=1
order by 1,2,3
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 2298
--2298 - Tasks Count in the Weekend

--first find the count of weekdays and working days by using WEEKDAY
--use daysofweek() to get days in numbers and take its sum to get count
select sum(DAYOFWEEK(submit_date) in (5,6)) as weeknd_cnt,
sum(DAYOFWEEK(submit_date) not in (5,6)) as working_cnt
from tasks;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem :1527
--1527 - Patients With a Condition

--use like operator
select patient_id,patient_name,conditions from patients
where conditions like '% DIAB1%' or conditions like 'DIAB1%';
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem :1341
--1341 - Movie Rating

--first join three tables, and then union name and title
with cte as(
    select mr.*,u.name,m.title from movierating mr
    left join users u
    on mr.user_id=u.user_id
    left join movies m
    on mr.movie_id=m.movie_id
)
(select name as results from cte
group by name
order by count(*) desc,name asc limit 1)
union all
(select title from cte
where date_format(created_at,'%Y-%m')='2020-02'
group by title
order by avg(rating) desc,title limit 1)

--second solution using union only
(
    select name as results
    from
        users
        join movierating using (user_id)
    group by user_id
    order by count(1) desc, name
    limit 1
)
union all
(
    select title
    from
        movierating
        join movies using (movie_id)
    where date_format(created_at, '%y-%m') = '2020-02'
    group by movie_id
    order by avg(rating) desc, title
    limit 1
);
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem :1179
--1179 - Reformat Department Table

--use case when with max() or sum()
select id,max(case when month='Jan' then revenue else null end) as Jan_Revenue,
max(case when month='Feb' then revenue else null end) as Feb_Revenue,
max(case when month='Mar' then revenue else null end) as Mar_Revenue,
max(case when month='Apr' then revenue else null end) as Apr_Revenue,
max(case when month='May' then revenue else null end) as May_Revenue,
max(case when month='Jun' then revenue else null end) as Jun_Revenue,
max(case when month='Jul' then revenue else null end) as Jul_Revenue,
max(case when month='Aug' then revenue else null end) as Aug_Revenue,
max(case when month='Sep' then revenue else null end) as Sep_Revenue,
max(case when month='Oct' then revenue else null end) as Oct_Revenue,
max(case when month='Nov' then revenue else null end) as Nov_Revenue,
max(case when month='Dec' then revenue else null end) as Dec_Revenue
from department
group by id
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem :262
--262. Trips and Users

--use left join on users u1,u2 and trips table, then calculate Percentage
select
request_at as day,
round(avg(status != 'completed'), 2) as 'cancellation rate'
from trips as t
join users as u1 on (t.client_id = u1.users_id and u1.banned = 'no')
join users as u2 on (t.driver_id = u2.users_id and u2.banned = 'no')
where request_at between '2013-10-01' and '2013-10-03'
group by request_at;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problem :601
--601 - Human Traffic of Stadium

--first take diff by using row_number - id, for consecutive ids it will give same diff
--use condition where count(*)>=3 as mentioned in pblm statement and then order it by visit_date.
with cte as(
select *,id - (ROW_NUMBER() OVER (ORDER BY id)) AS rk
from Stadium
where people >= 100)
select id,visit_date,people
from cte
where rk in(
select rk
from cte
group by rk
having count(*)>=3)
order by visit_date;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------