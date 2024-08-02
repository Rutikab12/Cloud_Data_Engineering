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
drop table if exists vfscmuat_dh_lake_comms_ie_dev_staging_s.cinema;
create table vfscmuat_dh_lake_comms_ie_dev_staging_s.cinema(
  seat_id int64,
  free int64
);
insert into vfscmuat_dh_lake_comms_ie_dev_staging_s.cinema values(1,1),(2,0),(3,1),(4,1),(5,1);
select * from vfscmuat_dh_lake_comms_ie_dev_staging_s.cinema;

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
------------------------------------------------------------------------------------------------------------------------------------
