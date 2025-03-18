a. What is the most popular breed in the created Orders?
WITH breed_counts AS (
    SELECT 
        dp.pet_breed,
        COUNT(fo.order_sk) AS order_cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(fo.order_sk) DESC) AS rnk
    FROM odl.dim_pets dp
    INNER JOIN odl.fact_orders fo 
        ON dp.pet_sk = fo.pet_sk
    GROUP BY dp.pet_breed
)
SELECT pet_breed, order_cnt
FROM breed_counts
WHERE rnk = 1;

b. How many Orders get abandoned? (we consider an Order abandoned
if it has not been completed within 30 days of creation)

SELECT 
    COUNT(DISTINCT fo.order_id) total_orders,
	COUNT(DISTINCT foc.order_id) total_completed_orders, 
	COUNT(DISTINCT (CASE WHEN foc.order_id IS NULL THEN fo.order_id ELSE NULL END)) total_abandoned_orders,
FROM odl.fact_orders fo
LEFT JOIN odl.fact_order_complete foc 
    ON fo.order_id = foc.order_id
    AND foc.order_complete_date BETWEEN fo.order_create_date AND DATEADD(DAY, 30, fo.order_create_date);


c. What is the completion rate for each type of “intent”?


WITH total_data AS (
SELECT 
    fo.intent,
    COUNT(DISTINCT fo.order_id) total_orders,
	COUNT(DISTINCT foc.order_id) total_completed_orders, 
	COUNT(DISTINCT (CASE WHEN foc.order_id IS NULL THEN fo.order_id ELSE NULL END)) total_abandoned_orders,
FROM odl.fact_orders fo
LEFT JOIN odl.fact_order_complete foc 
    ON fo.order_id = foc.order_id
    AND foc.order_complete_date BETWEEN fo.order_create_date AND DATEADD(DAY, 30, fo.order_create_date)
GROUP BY 
    fo.intent
)
SELECT 
	intent,
    total_completed_orders * 1.0 / total_orders AS completion_rate,  
    total_abandoned_orders * 1.0 / total_orders AS abandonment_rate  
 from total_data;
	
d. What is the most popular pet name?

WITH pet_name_counts AS (
    SELECT 
        dp.pet_name,
        COUNT(DISTINCT fo.order_sk) AS order_cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(fo.order_sk) DESC) AS rnk
    FROM odl.dim_pets dp
    INNER JOIN odl.fact_orders fo 
        ON dp.pet_sk = fo.pet_sk
    GROUP BY dp.pet_name
)
SELECT pet_name, order_cnt
FROM pet_name_counts
WHERE rnk = 1;

e. What is the average deviation of the number of created Order per day
from the 5 day rolling average?

WITH daily_orders AS (
    SELECT 
        fo.order_create_date,
        COUNT(DISTINCT fo.order_id) AS daily_orders
    FROM odl.fact_orders fo
    GROUP BY fo.order_create_date
),
rolling_avg AS (
    SELECT
        order_create_date,
        daily_orders,
        AVG(daily_orders) OVER (ORDER BY order_create_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_day_avg
    FROM daily_orders
)
SELECT 
	order_create_date,
    daily_orders,
    rolling_5_day_avg,
    (daily_orders - rolling_5_day_avg) AS average_deviation
FROM rolling_avg; 