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
