--ADVANCED SQL 50 LEETCODE

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 1440
--evaluate boolen expression

--first tale values of variables in expression table using left join
--and then use case when
select e.*,v1.value as lto,v2.value as rto,
case when e.operator='>' then if((v1.value > v2.value)=0,'false','true')
when e.operator='<' then if((v1.value < v2.value)=0,'false','true')
when e.operator='=' then if((v1.value = v2.value)=0,'false','true')
else null end as value
from expressions e
left join variables v1
on e.left_operand=v1.name
left join Variables v2
on e.right_operand=v2.name;

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 1212
--Team Scores in Football Tournament

--first do the left join on team_id=guest_id and host_id we will get all team_id.
--group by team_id and use sum(case when)
--order by points desc,team_id

with cte as(
select m.*,t.team_id,t.team_name,
SUM(
        CASE
            WHEN team_id = host_team
            AND host_goals > guest_goals THEN 3
            WHEN team_id = guest_team
            AND guest_goals > host_goals THEN 3
            WHEN host_goals = guest_goals THEN 1
            ELSE 0
        END
    ) AS num_points
from Teams t
left join Matches m
on t.team_id=m.host_team or t.team_id=m.guest_team
group by t.team_id)

select team_id,team_name,num_points from cte
order by 3 desc,1;
-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 1890
--Last Login by user

--use year() to get year=2020 and take max(time_stamp)
select user_id,max(time_stamp) as last_stamp from logins
where year(time_stamp)=2020
group by user_id;

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 511
--Game Play Analysis I

--use min() to get first login date and group by plyaer_id
select player_id,min(event_date) as first_login from activity group by 1;

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 1571
--Warehouse Volume

--do inner join on product_id, sum(w*l*h*units) as volume
--group by warehouse_name

select w.name as warehouse_name,
sum(width*length*height*units) as volume
from Products p
inner join Warehouse w
on p.product_id=w.product_id
group by 1;

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 586
--Customer Placing Largest Number of Orders

--using cte and selecting max cause if duplicate count comes, it should not be missed.
--first find count of orders per customer_number and then take cnt=max(count)
with cte as(
select customer_number,count(order_number) as cnt from orders
group by customer_number)s
select customer_number from cte
where cnt=(select max(cnt) from cte);

-------------------------------------------------------------------------------------------------------------------------------------------
--leetcode : 1741
--Find Total Time Spent By Each Employee

--first do group by event_Date and emp_id, then sum(out-in) to get total_time
select event_day as day,emp_id,sum(out_time-in_time) as total_time from employees
group by emp_id,event_day;
-------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1173
--Immediate Food Delivery I

--use sum(compare order= prefer date), then take count of delivery_id and do percentage calculation.
select round(sum(order_date=customer_pref_delivery_date)/count(1)*100,2) as immediate_percentage from Delivery;
-------------------------------------------------------------------------------------------------------------------------------------------
--LeetCode Problems: 1445
--Apple & Oranges

--use case when and take diff
with cte as(
select sale_date,
sum(case when fruit='apples' then sold_num else 0 end) as apple_count,
sum(case when fruit='oranges' then sold_num else 0 end) as orange_count 
from cricket_dataset.sales
group by sale_date)
select sale_date,(apple_count-orange_count) as diff from cte
-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------