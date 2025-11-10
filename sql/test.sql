-- 1) Total sales & profit by region (highest to lowest)
-- Useful to see which regions generate most revenue and profit.
SELECT
  l.region,
  COUNT(DISTINCT o.order_id)        AS orders,
  SUM(fs.quantity)                  AS total_qty,
  ROUND(SUM(fs.sales)::numeric, 2)  AS total_sales,
  ROUND(SUM(fs.profit)::numeric, 2) AS total_profit,
  ROUND(100.0 * SUM(fs.profit) / NULLIF(SUM(fs.sales),0), 2) AS profit_margin_pct
FROM fact_sales fs
JOIN dim_location l    ON fs.location_id = l.location_id
JOIN dim_order o       ON fs.order_id = o.order_id
GROUP BY l.region
ORDER BY total_sales DESC;



-- 2) Monthly sales trend (year, month) — time series of total sales
-- Shows sales by year/month. Use ORDER BY to get chronological results.
SELECT
  o.order_year,
  o.order_month,
  SUM(fs.sales) AS month_sales,
  SUM(fs.profit) AS month_profit,
  COUNT(DISTINCT o.order_id) AS orders_count
FROM fact_sales fs
JOIN dim_order o ON fs.order_id = o.order_id
GROUP BY o.order_year, o.order_month
ORDER BY o.order_year, o.order_month;



-- 3) Top 10 customers by lifetime sales (customer_key + name)
-- Shows customers bringing highest revenue.
SELECT
  c.customer_key,
  c.customer_name,
  COUNT(DISTINCT o.order_id) AS orders_count,
  SUM(fs.sales) AS total_sales,
  SUM(fs.profit) AS total_profit
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_id = c.customer_id
JOIN dim_order o     ON fs.order_id = o.order_id
GROUP BY c.customer_key, c.customer_name
ORDER BY total_sales DESC
LIMIT 10;



-- 4) Top 10 products by profit (product_key + name)
SELECT
  p.product_key,
  p.product_name,
  p.category,
  p.sub_category,
  SUM(fs.sales)   AS total_sales,
  SUM(fs.profit)  AS total_profit,
  SUM(fs.quantity) AS total_qty
FROM fact_sales fs
JOIN dim_product p ON fs.product_id = p.product_id
GROUP BY p.product_key, p.product_name, p.category, p.sub_category
ORDER BY total_profit DESC
LIMIT 10;



-- 5) Sales & profit by ship_mode (to analyze shipping/channel impact)
SELECT
  o.ship_mode,
  SUM(fs.sales)   AS total_sales,
  SUM(fs.profit)  AS total_profit,
  AVG(fs.discount) AS avg_discount,
  COUNT(DISTINCT o.order_id) AS orders_count
FROM fact_sales fs
JOIN dim_order o ON fs.order_id = o.order_id
GROUP BY o.ship_mode
ORDER BY total_sales DESC;