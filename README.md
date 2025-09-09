# 📦 Olist E-commerce Data Warehouse & Analytics
# 🔹 Project Description

This project builds a scalable end-to-end data warehouse and analytics pipeline for the Olist e-commerce dataset
.

The goal is to transform raw marketplace data (orders, customers, sellers, products, payments, reviews, and geolocations) into business-ready insights using the Bronze → Silver → Gold (Fact) architecture.

Bronze Layer: Ingest raw datasets with minimal processing to preserve source-of-truth.

Silver Layer: Clean, standardize, and integrate data across entities (customers, sellers, products, orders, payments, and reviews).

Gold Layer (Fact Tables): Build optimized fact and dimension tables for reporting, KPIs, and advanced analytics.

This structured pipeline ensures reproducibility, auditability, and scalability, making it easy for analysts, data engineers, and business teams to explore trends, answer business questions, and power dashboards.

# 🔹 Key Features

✅ ETL pipeline using SQL (Bronze, Silver, and Fact models).

✅ Well-defined schema for Customers, Orders, Products, Sellers, Payments, Items, Reviews, and Geolocations.

✅ Fact & dimension tables supporting KPIs like GMV, AOV, cancellations, delivery times, and review scores.

✅ Sample analytical queries for top products, seller performance, payment adoption, and delivery delays.

✅ Reproducible setup with DuckDB/PostgreSQL support.

✅ Visual dashboards (Tableau/Power BI) for business storytelling.

# 🔹 Business Impact

📊 Identifies top-performing products & categories.

🚚 Highlights delivery bottlenecks by region & seller.

💳 Analyzes payment method adoption vs cancellation rates.

⭐ Correlates customer satisfaction (reviews) with delivery speed.

🔮 Provides foundation for LTV, churn, and recommendation models.

# Why this project matters 

Turns raw marketplace logs into actionable business signals (top-products, friction points by seller/region, payment behaviour, NPS-like review trends).

Designed for reproducibility: raw → bronze → silver → fact model so downstream dashboards and ML features are stable and auditable.

Enables immediate business decisions: optimize shipping, prioritize sellers with high cancellations, improve payment routing.

# Project structure & step-by-step process

Use the bronze → silver → gold/fact pattern.

1) Bronze: Raw ingestion (audit-first)

Files: Bronze_Extraction.sql, Orders.sql, Customers.sql, Sellers.sql, Products.sql, items.sql, payments.sql, Reviews.sql, Geolocations.sql.

Purpose: ingest raw CSV/JSON/SQL dumps into raw tables with metadata columns:

__ingest_timestamp, __source_filename, __row_id_hash

Best practice: never overwrite; always append with batch id.

2) Silver: Cleaning & canonicalization

File: 1. Silver_Extraction.sql

Tasks performed:

Standardize date/time zones, parse timestamps to TIMESTAMP type.

Normalize address fields (city/state), deduplicate customers by email/phone heuristics.

Normalize product categories and map inconsistent strings to canonical categories.

Validate numeric fields (prices, freight_value) and repair obvious errors.

Output: canonical tables silver.customers, silver.orders, silver.items, etc.

3) Gold / Fact table: reporting model

File: fact_table.sql

Purpose: create fact_orders (one row = order-item or order depending on design), dimension tables dim_customers, dim_products, dim_sellers, dim_date.

Key facts: order_value, freight_value, delivery_delay_days, order_status, payment_installments.

4) Analysis layer (Tableau dashboards)

Tableau connect to my_olist.db and produce:

KPI dashboards (daily/weekly GMV, orders, AOV)

Cohort/LTV analyses

Seller performance & root-cause visualizations (late deliveries, cancellations)

Sentiment/qualitative analysis on reviews.

# Key analyses & sample SQLs 

Below are high-impact queries to answer business questions quickly.

# A) Top 10 products by revenue

      SELECT p.product_id, p.product_category_name, SUM(i.price * i.quantity) AS revenue, COUNT(DISTINCT o.order_id) AS orders

      FROM silver.items i

      JOIN silver.products p USING(product_id)

      JOIN silver.orders o USING(order_id)

      GROUP BY p.product_id, p.product_category_name

      ORDER BY revenue DESC

      LIMIT 10;

# B) GMV & orders by week (time series)

      SELECT DATE_TRUNC('week', o.order_purchase_timestamp) AS week,

      COUNT(DISTINCT o.order_id) AS orders,
       
      SUM(i.price * i.quantity) AS gmv
             
      FROM silver.orders o

      JOIN silver.items i USING(order_id)

      GROUP BY 1

      ORDER BY 1;

# C) Average delivery delay by state 

      SELECT g.state, AVG(DATE_PART('day', o.order_delivered_customer_date - o.order_delivered_carrier_date)) AS avg_delivery_days

      FROM silver.orders o

      JOIN silver.geolocations g ON o.customer_zip_code_prefix = g.zip_prefix

      WHERE o.order_delivered_customer_date IS NOT NULL

      GROUP BY g.state

      ORDER BY avg_delivery_days DESC;

# D) Payment method adoption & cancellations correlation

SELECT p.payment_type, COUNT(DISTINCT o.order_id) AS orders,

      SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT o.order_id) AS cancel_rate
       
      FROM silver.orders o

      JOIN silver.payments p USING(order_id)

      GROUP BY p.payment_type

      ORDER BY cancel_rate DESC;

Tip: Run these queries from a notebook, convert to visual charts (bar, time-series, heatmap) and include them in stakeholder slides.

# Tests & data quality checks

Add SQL/unit tests for:

Primary key uniqueness (e.g. order_id unique in silver.orders)

No NULLs in key dimension foreign keys (fact_orders.customer_id is not null)

Reasonability checks: price >= 0, freight_value >= 0, order_purchase_timestamp <= order_delivered_customer_date (except pending or canceled)

Row count delta tests between bronze → silver with summary logs.

Implement simple checks as queries in sql/tests/ and call them in CI.

# Deliverables & how to present to stakeholders

One-page KPI report: GMV, Orders, AOV, Cancellation Rate, Average Delivery Days, NPS proxy (avg review score).

Top 3 action items (data-driven):

Prioritize onboarding improvements for sellers in states with the highest delivery delays.

Rebalance payment routing for methods with high cancellation rates.

Target outreach to customers in high churn cohorts identified via LTV analysis.

Slide template: include cover, KPIs, 2 charts (trend + geo heatmap), 1 root-cause analysis slide, recommended next steps.

Repository artifacts: SQL ETL, fact table, notebooks with charts, exportable CSVs for BI tools (PowerBI/Tableau).
