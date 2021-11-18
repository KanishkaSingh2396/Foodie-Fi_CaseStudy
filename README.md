# Foodie-Fi_CaseStudy

--A. Customer Journey
--Based off the 8 sample customers provided in the sample from the subscriptions table, write a brief description about each customer’s onboarding journey.
```sql
SELECT customer_id, p.plan_id, start_date, plan_name
FROM foodie_fi.subscriptions s
join foodie_fi.plans p
on s.plan_id = p.plan_id
order by customer_id, plan_id, start_date
```
![image](https://user-images.githubusercontent.com/89623051/142484639-02fb4f7a-d3fa-4140-a990-0f05be9d7400.png)

--Part B: Data Analysis Questions

Question 1 How many customers has Foodie-Fi ever had?

```sql
select count(distinct customer_id) from foodie_fi.subscriptions
```
![image](https://user-images.githubusercontent.com/89623051/142484773-4293e10e-2c48-4ec9-8612-bb64ee8ee931.png)

Question 2 What is the monthly distribution of trial plan start_date values for our dataset - use the start of the month as the group by value

```sql
select
  DATE_TRUNC('MONTH', start_date) as month_start_date,
  count(plan_id)
from foodie_fi.subscriptions
where plan_id = 0
group by month_start_date
order by month_start_date
``` 
![image](https://user-images.githubusercontent.com/89623051/142485000-a0ea1814-2798-478e-97c0-ea1c2861b758.png)

Question 3 What plan start_date values occur after the year 2020 for our dataset? Show the breakdown by count of events for each plan_name

```sql
select  
  p.plan_id,
  plan_name,
  count(*) as events
from foodie_fi.subscriptions s
join foodie_fi.plans p
  on s.plan_id = p.plan_id
where start_date >= '2021-01-01'
group by p.plan_id, plan_name
order by p.plan_id asc
``` 
![image](https://user-images.githubusercontent.com/89623051/142485133-87246a7d-ea46-4f07-a718-1aa43d56c3a4.png)


Question 4 What is the customer count and percentage of customers who have churned rounded to 1 decimal place?

```sql
with base_tb as(
select 
  count(case
    when plan_id = 4 then customer_id
  end) as churn_count,
  count(case
    when plan_id = 0 then customer_id
  end) as total_count
from foodie_fi.subscriptions)

select total_count, churn_count, round(100*(churn_count::numeric/total_count::numeric),1) as churn_percentage
from base_tb; 
```
![image](https://user-images.githubusercontent.com/89623051/142485362-cb7aee3e-a786-4472-bbee-4b17a8e1d605.png)


Question 5 How many customers have churned straight after their initial free trial - what percentage is this rounded to 1 decimal place?

```sql
with base as(
select customer_id, start_date, plan_id, row_number() over(partition by customer_id order by plan_id asc) as rank_plan
from foodie_fi.subscriptions)
select 
count(case when plan_id = 4 then 1 end) as free_trial_churn,
round(100*(count(case when plan_id = 4 then 1 end)::numeric/count(*)::numeric),2) as churn_percentage
from base where rank_plan = 2
```
![image](https://user-images.githubusercontent.com/89623051/142485466-394be342-71be-4a18-8214-e56f39b1de6a.png)

6. What is the number and percentage of customer plans after their initial free trial?

```sql
WITH ranked_plans AS (
  SELECT customer_id, plan_id,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY plan_id asc) AS plan_rank
  FROM foodie_fi.subscriptions)
SELECT plans.plan_id, plans.plan_name,
  COUNT(*) AS customer_id,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ranked_plans
INNER JOIN foodie_fi.plans
  ON ranked_plans.plan_id = plans.plan_id
WHERE plan_rank = 2
GROUP BY plans.plan_id, plans.plan_name
ORDER BY plans.plan_id;
```
![image](https://user-images.githubusercontent.com/89623051/142485681-e3812bbd-c5de-4de9-b57f-fca98248843a.png)

--7. What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31?

```sql
WITH valid_subscriptions AS (
  SELECT
    customer_id,
    plan_id,
    start_date,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY start_date DESC
    ) AS plan_rank
  FROM foodie_fi.subscriptions
  WHERE start_date <= '2020-12-31'
),

summarised_tb as (
select* from valid_subscriptions where plan_rank = 1)

select plan_id, count(*)as customers, round(100*COUNT(*) / SUM(COUNT(*)) OVER (),1) as percentage
from summarised_tb 
group by plan_id
```
![image](https://user-images.githubusercontent.com/89623051/142485836-50a4f90b-f275-40c2-ae9f-b540126f9197.png)

Question 8 How many customers have upgraded to an annual plan in 2020?

```sql
WITH valid_subscriptions AS (
  SELECT
    customer_id,
    plan_id,
    start_date,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY start_date DESC
    ) AS plan_rank
  FROM foodie_fi.subscriptions
  WHERE start_date <= '2020-12-31' and start_date >= '2020-01-01' and plan_id = 3
)
select count(*) as annual_customers from valid_subscriptions where plan_rank = 1
```
![image](https://user-images.githubusercontent.com/89623051/142486014-2ede96da-8f45-41e7-843f-b98b8c6b2d4a.png)

--Question 9 How many days on average does it take for a customer to an annual plan from the day they join Foodie-Fi?

```sql
with base as (
select 
  customer_id, 
  plan_id, 
  start_date, 
  (lead(start_date) over(partition by customer_id order by start_date asc)) as lead_start_date
from foodie_fi.subscriptions
where plan_id in(0,3))

select ceiling(avg(lead_start_date - start_date)) as avg_days from base where lead_start_date is not null
```
![image](https://user-images.githubusercontent.com/89623051/142486167-c53a4409-d96a-4d6e-ba8e-138162da0baf.png)

Question 11 How many customers downgraded from a pro monthly to a basic monthly plan in 2020? downgraded from 2 to 1

```sql
with base as (select 
  customer_id, 
  plan_id, 
  start_date, 
  (lead(plan_id) over(partition by customer_id order by start_date asc)) as lead_plan_id,
  (lead(start_date) over(partition by customer_id order by start_date asc)) as lead_start_date
from foodie_fi.subscriptions
where extract(year from start_date) = 2020)

select count(*) from base where lead_plan_id is not null and lead_plan_id = 2 and plan_id = 1
```
![image](https://user-images.githubusercontent.com/89623051/142486289-9f6a1666-d2e0-4b39-9164-3c80d4ca0533.png)

-------------------------------------
Question 10 Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)

```sql
CREATE Temp TABLE interval(
   month_interval int,
   breakdown_period varchar)
INSERT INTO interval (month_interval, breakdown_period)
VALUES
(1, '0 - 30 days'),
(2, '30 - 60 days'),	
(3, '60 - 90 days'),	
(4, '90 - 120 days'),
(5, '120 - 150 days'),	
(6, '150 - 180 days'),	
(7, '180 - 210 days'),	
(8, '210 - 240 days'),	
(9, '240 - 270 days'),	
(10, '270 - 300 days'),	
(11, '300 - 330 days'),	
(12, '330 - 360 days');

with base as (
select 
  customer_id, 
  plan_id, 
  start_date, 
  (lead(start_date) over(partition by customer_id order by start_date asc)) as lead_start_date
from foodie_fi.subscriptions
where plan_id in(0,3)),

tb as (select *, (lead_start_date - start_date) as diff from base where lead_start_date is not null),

tb1 as (select 
  (case 
  when diff < 30 then 1
  when diff < 60 then 2
  when diff <90  then 3
  when diff <120 then 4
  when diff <150 then 5
  when diff <180 then 6
  when diff <210 then 7
  when diff <240 then 8
  when diff <270 then 9
  when diff <300 then 10
  when diff <330 then 11
  when diff <360 then 12
  else diff
  end) as month_interval,
  count(*) as customers
from tb 
group by month_interval
order by month_interval)

select breakdown_period, customers
from tb1
join interval
on tb1.month_interval = interval.month_interval
```sql

![image](https://user-images.githubusercontent.com/89623051/142486674-2d2197bd-e6ed-4f6b-b9ea-04f24881172e.png)

----------------------------------------------------------------------------------
Part B Challenge Payment Question
----------------------------------------------------------------------------------

The Foodie-Fi team wants you to create a new payments table for the year 2020 that includes amounts paid by each customer in the subscriptions table with the following requirements:
monthly payments always occur on the same day of month as the original start_date of any monthly paid plan
upgrades from basic to monthly or pro plans are reduced by the current paid amount in that month and start immediately
upgrades from pro monthly to pro annual are paid at the end of the current billing period and also starts at the end of the month period
once a customer churns they will no longer make payments

```sql
with base as (
select* from foodie_fi.subscriptions where plan_id != 0 and  date_part ('year', start_date) = 2020 order by customer_id, start_date),
lead_plans as(select*, 
  lead(plan_id) over( partition by customer_id order by start_date) as lead_plan_id,
  lead(start_date) over(partition by customer_id order by start_date)as lead_start_date
from base )

select plan_id, lead_plan_id, count(*) from lead_plans group by plan_id, lead_plan_id ORDER BY plan_id, lead_plan_id;
```
![image](https://user-images.githubusercontent.com/89623051/142486899-af18135f-52be-4388-a467-2daa7c43953b.png)

-------------------------------------------
-- case 1: non churn monthly customers

```sql
WITH lead_plans AS (
SELECT customer_id,plan_id,start_date,
  LEAD(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_plan_id,
  LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_start_date
FROM foodie_fi.subscriptions WHERE DATE_PART('year', start_date) = 2020 AND plan_id != 0),
case_1 as(select customer_id, plan_id, start_date, 12 - extract('month' from start_date)::int as month_diff
from lead_plans where lead_plan_id is null and plan_id != 3 and plan_id !=4),
payment_tb as (select *, month_diff, (start_date + generate_series(0, month_diff)* interval '1 month') as payment_date from case_1),

-- union final table
output AS (
  SELECT * FROM payment_tb
)
SELECT
  customer_id,
  plans.plan_id,
  plans.plan_name,
  payment_date,
  -- price deductions are applied here
  CASE
    WHEN output.plan_id IN (2, 3) AND
      LAG(output.plan_id) OVER w = 1
    THEN plans.price - 9.90
    ELSE plans.price
    END AS amount,
  RANK() OVER w AS payment_order
FROM output
INNER JOIN foodie_fi.plans
  ON output.plan_id = plans.plan_id

WINDOW w AS (
  PARTITION BY output.customer_id
  ORDER BY payment_date, customer_id
);
```
![image](https://user-images.githubusercontent.com/89623051/142487290-1bc14046-67df-44f1-aef8-8d7c8aa18fc4.png)

-------------------------------------
--case 2 churn customers

```sql
WITH lead_plans AS (
SELECT customer_id,plan_id,start_date,
  LEAD(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_plan_id,
  LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_start_date
FROM foodie_fi.subscriptions WHERE plan_id != 0 and DATE_PART('year', start_date) = 2020),

month_tb as (select customer_id,plan_id,start_date, lead_start_date, extract(month from  AGE(lead_start_date - 1, start_date))::INTEGER AS month_diff
from lead_plans where lead_plan_id = 4),

payment_tb as (select customer_id,plan_id, month_diff, start_date + generate_series(0, month_diff)* interval '1 month' as payment_date
from month_tb),

-- union final table
output AS (
  SELECT * FROM payment_tb
)
SELECT
  customer_id,
  plans.plan_id,
  plans.plan_name,
  payment_date,
  -- price deductions are applied here
  CASE
    WHEN output.plan_id = 2 AND
      LAG(output.plan_id) OVER w = 1
    THEN plans.price - 9.90
    ELSE plans.price
    END AS amount,
  RANK() OVER w AS payment_order
FROM output
INNER JOIN foodie_fi.plans
  ON output.plan_id = plans.plan_id

WINDOW w AS (
  PARTITION BY output.customer_id
  ORDER BY payment_date, customer_id
);
```
![image](https://user-images.githubusercontent.com/89623051/142487467-d5a91175-e7f0-4d27-aae1-193b0e1e05c1.png)

-- case 3: customers who move from basic to pro plans

```sql
WITH lead_plans AS (
SELECT customer_id,plan_id,start_date,
  LEAD(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_plan_id,
  LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_start_date
FROM foodie_fi.subscriptions WHERE plan_id != 0 and plan_id != 4  and DATE_PART('year', start_date) = 2020),

month_tb as (select customer_id,start_date,plan_id, lead_plan_id,lead_start_date, extract(month from AGE(lead_start_date-1, start_date))::int as month_diff 
from lead_plans where lead_plan_id = 2 or lead_plan_id = 3 and plan_id = 1),

payment_tb as (select customer_id,plan_id, (start_date + generate_series(0, month_diff)* interval '1 month') as payment_date
from month_tb),

-- union final table
output AS (
  SELECT * FROM payment_tb
)
SELECT
  customer_id,
  plans.plan_id,
  plans.plan_name,
  payment_date,
  price as amount,
  RANK() OVER w AS payment_order
FROM output
INNER JOIN foodie_fi.plans
  ON output.plan_id = plans.plan_id

WINDOW w AS (
  PARTITION BY output.customer_id
  ORDER BY payment_date, customer_id
);
```

![image](https://user-images.githubusercontent.com/89623051/142487611-d874000d-1a1f-4e65-baab-377638730f34.png)

---------------------------------------------
--case 4: pro monthly customers who move up to annual plans


```sql
WITH lead_plans AS (
SELECT customer_id,plan_id,start_date,
  LEAD(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_plan_id,
  LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_start_date
FROM foodie_fi.subscriptions WHERE plan_id != 0 and DATE_PART('year', start_date) = 2020),

month_tb as (select customer_id, plan_id, lead_plan_id, start_date, extract(month from age(lead_start_date - 1, start_date))::int as month_diff
from lead_plans where plan_id = 2 and lead_plan_id = 3),
payment_tb as (select customer_id,plan_id, (start_date + generate_series(0, month_diff)* interval '1 month') as payment_date from month_tb),

-- union final table
output AS (
  SELECT * FROM payment_tb
)
SELECT
  customer_id,
  plans.plan_id,
  plans.plan_name,
  payment_date,
  price AS amount,
  RANK() OVER w AS payment_order
FROM output
INNER JOIN foodie_fi.plans
  ON output.plan_id = plans.plan_id

WINDOW w AS (
  PARTITION BY output.customer_id
  ORDER BY payment_date, customer_id
);
```
![image](https://user-images.githubusercontent.com/89623051/142487735-e76216be-60b3-4c30-9f6e-020412b3ca9e.png)

----------------------------------------------------
--case 5: annual_pro_payments, id = 3

```sql
WITH lead_plans AS (
SELECT customer_id,plan_id,start_date,
  LEAD(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_plan_id,
  LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS lead_start_date
FROM foodie_fi.subscriptions WHERE plan_id != 0 and DATE_PART('year', start_date) = 2020),

month_tb as(select customer_id,plan_id,start_date, (12 - extract(month from start_date)::int) as month_diff
from lead_plans where plan_id = 3),

payment_tb as (select customer_id, plan_id, start_date, (start_date + generate_series(0, month_diff)* interval '1 month') as payment_date
from month_tb),

-- union final table
output AS (
  SELECT * FROM payment_tb
)
SELECT
  customer_id,
  plans.plan_id,
  plans.plan_name,
  payment_date,
  price AS amount,
  RANK() OVER w AS payment_order
FROM output
INNER JOIN foodie_fi.plans
  ON output.plan_id = plans.plan_id

WINDOW w AS (
  PARTITION BY output.customer_id
  ORDER BY payment_date, customer_id
);
```
![image](https://user-images.githubusercontent.com/89623051/142487892-19302adb-8a8d-49c6-af2e-590ca7bf75e5.png)
