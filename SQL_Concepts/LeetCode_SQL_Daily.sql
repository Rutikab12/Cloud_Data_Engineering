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
LeetCode Problems: 182
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