-- Product performance mart (incremental)
-- Data Lineage: stg_orders -> marts/product_performance.sql
-- Grain: one row per product with sales, return, and revenue metrics

{{ config(
    materialized='incremental',
    schema='marts',
    unique_key='product_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH base AS (
    SELECT
        product_id,
        product_name,
        category,
        list_price,
        COUNT(order_id)                                 AS total_orders,
        SUM(quantity)                                   AS total_units_sold,
        SUM(total_amount)                               AS total_revenue,
        AVG(total_amount)                               AS avg_order_value,
        COUNT(CASE WHEN status = 'returned' THEN 1 END) AS total_returns,
        COUNT(DISTINCT user_id)                         AS unique_buyers,
        MIN(order_date)                                 AS first_sale_date,
        MAX(order_date)                                 AS last_sale_date
    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    {% if is_incremental() %}
        AND order_time >= (SELECT MAX(last_sale_date) FROM {{ this }})
    {% endif %}
    GROUP BY product_id, product_name, category, list_price
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_revenue DESC)           AS revenue_rank_global,
        RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank_in_category,
        ROUND((total_returns::numeric / NULLIF(total_orders, 0)) * 100, 2) AS return_rate_pct
    FROM base
)

SELECT
    product_id,
    product_name,
    category,
    list_price,
    total_orders,
    total_units_sold,
    ROUND(total_revenue::numeric, 2)    AS total_revenue,
    ROUND(avg_order_value::numeric, 2)  AS avg_order_value,
    total_returns,
    return_rate_pct,
    unique_buyers,
    first_sale_date,
    last_sale_date,
    revenue_rank_global,
    revenue_rank_in_category,
    CURRENT_TIMESTAMP                   AS dbt_updated_at
FROM ranked
