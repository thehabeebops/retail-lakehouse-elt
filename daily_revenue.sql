SELECT order_date, count(DISTINCT order_id) AS orders, sum(qty) AS units, round(sum(line_amount), 2) AS revenue
FROM {fact}
WHERE status <> 'cancelled'
GROUP BY 1
ORDER BY 1
