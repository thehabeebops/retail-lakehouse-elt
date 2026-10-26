WITH activity AS (
    SELECT DISTINCT c.customer_id, f.order_date
    FROM {fact} f JOIN {dim_customer} c USING (customer_sk)
    WHERE f.status <> 'cancelled'
),
first_order AS (SELECT customer_id, min(order_date) AS first_date FROM activity GROUP BY 1)
SELECT date_trunc('week', fo.first_date) AS cohort_week,
       date_diff('week', date_trunc('week', fo.first_date), date_trunc('week', a.order_date)) AS weeks_since_first_order,
       count(DISTINCT a.customer_id) AS active_customers
FROM activity a JOIN first_order fo USING (customer_id)
GROUP BY 1, 2
ORDER BY 1, 2
