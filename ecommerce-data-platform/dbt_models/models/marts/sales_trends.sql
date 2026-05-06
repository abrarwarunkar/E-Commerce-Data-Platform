-- Sales trends mart (incremental)
-- Data Lineage: stg_orders -> marts/sales_trends.sql
-- Grain: one row per day with daily, WoW, and MoM metrics

{{ config(
    materialized='incremental',
    schema='marts',
    unique_key='period',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH daily AS (
    SELECT
        order_date                                          AS period,
        year,
        quarter,
        month,
        week_of_year,
        day_of_week,
        is_weekend,
        COUNT(order_id)                                     AS total_orders,
        SUM(quantity)                                       AS total_units_sold,
        ROUND(SUM(total_amount)::numeric, 2)                AS total_revenue,
        ROUND(AVG(total_amount)::numeric, 2)                AS avg_order_value,
        COUNT(DISTINCT user_id)                             AS unique_customers,
        COUNT(DISTINCT product_id)                          AS unique_products_sold
    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    {% if is_incremental() %}
        AND order_date > (SELECT MAX(period) FROM {{ this }})
    {% endif %}
    GROUP BY order_date, year, quarter, month, week_of_year, day_of_week, is_weekend
),

with_lag AS (
    SELECT
        *,
        LAG(total_revenue, 1)  OVER (ORDER BY period) AS prev_day_revenue,
        LAG(total_revenue, 7)  OVER (ORDER BY period) AS prev_week_revenue,
        LAG(total_revenue, 30) OVER (ORDER BY period) AS prev_month_revenue
    FROM daily
)

SELECT
    period,
    year,
    quarter,
    month,
    week_of_year,
    day_of_week,
    is_weekend,
    total_orders,
    total_units_sold,
    total_revenue,
    avg_order_value,
    unique_customers,
    unique_products_sold,
    prev_day_revenue,
    CASE WHEN prev_day_revenue > 0
         THEN ROUND(((total_revenue - prev_day_revenue) / prev_day_revenue * 100)::numeric, 2)
    END                                              AS day_over_day_pct,
    CASE WHEN prev_week_revenue > 0
         THEN ROUND(((total_revenue - prev_week_revenue) / prev_week_revenue * 100)::numeric, 2)
    END                                              AS week_over_week_pct,
    CASE WHEN prev_month_revenue > 0
         THEN ROUND(((total_revenue - prev_month_revenue) / prev_month_revenue * 100)::numeric, 2)
    END                                              AS month_over_month_pct,
    CURRENT_TIMESTAMP                                AS dbt_updated_at
FROM with_lag
ORDER BY period DESC
