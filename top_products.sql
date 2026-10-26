SELECT p.sku, p.category, sum(f.qty) AS units, round(sum(f.line_amount), 2) AS revenue,
       rank() OVER (ORDER BY sum(f.line_amount) DESC) AS revenue_rank
FROM {fact} f
JOIN {dim_product} p USING (product_sk)
WHERE f.status <> 'cancelled'
GROUP BY 1, 2
ORDER BY revenue_rank
LIMIT 50
