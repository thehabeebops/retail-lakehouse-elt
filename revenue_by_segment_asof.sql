-- Segment is the one the customer had AT ORDER TIME (SCD2 point-in-time join), not today's segment.
SELECT date_trunc('week', f.order_date) AS week, c.segment,
       count(DISTINCT f.order_id) AS orders, round(sum(f.line_amount), 2) AS revenue
FROM {fact} f
JOIN {dim_customer} c USING (customer_sk)
WHERE f.status <> 'cancelled'
GROUP BY 1, 2
ORDER BY 1, 2
