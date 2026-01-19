View in SQL

it is an SQL obejct created over an SQL query
view do not store data, it just executes sql query underlying it.


1. security
2. makes it easier for complex queries

eg:
#creating a role
create role james
login
password "james";

#granting permission to role
grant select on order_summary to james;

create or replace view view_name
#here you can't change column name in case view already exists\

#renaming syntax for view
alter view order_summary rename column date to order_date;
alter view order_summary rename to order_summary_2;

#deleting the view
drop view order_summary_2;

-------------------------------------------------------------------------

Recursive query

syntax:

WITH [RECURSIVE] CTE_name AS
(
	select query (Non Recursive query or the Base Query)
	UNION [ALL]
	select query (Recursive query using CTE_name [with a termination condition])
)
select * from CTE_name;

first sql look for base query then it will execute second query and check termination condition and iteration continues.

eg:display numbers from 1-10 without using built in functions

with recursive numbers as
	(
		select 1 as n
		union
		select n+1 from numbers where n<10
	)
select * from numbers;
------------------------------------------------------------------------
eg: find the hirerachy of employees under a given manager 'Ashana'

with recursive emp_hierarchy
	(
		select id,emp_name,designation, 1 as lvl from emp_details where name='Ashana'
		union
		select E.id,E.emp_name,E.designation, H.lvl+1 as lvl from emp_hierarchy H
		join emp_details E
		ON H.id = E.manager_id
	)
	
--
select H2.id as emp_id,H2.name as emp_name, E2.name as manager_name, H2.lvl as level 
from emp_hierarchy H2
join emp details E2
ON E2.id=H2.manager_id;
-------------------------------------------------------------------------
eg:Find the hirerachy of managers for a given employee "David"

with recursive emp_hierarchy
	(
		select *, 1 as lvl from emp_details where name='David'
		union
		select E.*, H.lvl+1 as lvl from emp_hierarchy H
		join emp_details E
		ON H.manager_id = E._id
	)
select H2.id as emp_id,H2.name as emp_name, E2.name as manager_name, H2.lvl as level 
from emp_hierarchy H2
join emp details E2
ON E2.manager_id=H2.id;