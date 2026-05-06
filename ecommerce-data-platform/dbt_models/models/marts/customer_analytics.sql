-- Customer analytics mart (incremental)
-- Data Lineage: stg_orders + fact_events -> marts/customer_analytics.sql
-- Grain: one row per user with lifetime and period metrics

{{ config(
    materialized='incremental',
    schema='marts',
    unique_key='user_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH order_metrics AS (
    SELECT
        user_id,
        user_name,
        user_email,
        user_location,
        COUNT(order_id)                             AS total_orders,
        SUM(total_amount)                           AS lifetime_revenue,
        AVG(total_amount)                           AS avg_order_value,
        MIN(order_date)                             AS first_order_date,
        MAX(order_date)                             AS last_order_date,
        COUNT(DISTINCT category)                    AS distinct_categories,
        COUNT(CASE WHEN status = 'returned' THEN 1 END) AS total_returns,
        SUM(quantity)                               AS total_units_purchased
    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    {% if is_incremental() %}
        AND order_time >= (SELECT MAX(last_order_date) FROM {{ this }})
    {% endif %}
    GROUP BY user_id, user_name, user_email, user_location
),

recency_calc AS (
    SELECT
        user_id,
        CURRENT_DATE - MAX(order_date)::date        AS days_since_last_order
    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    GROUP BY user_id
),

segmentation AS (
    SELECT
        om.user_id,
        CASE
            WHEN om.lifetime_revenue >= 5000 THEN 'VIP'
            WHEN om.lifetime_revenue >= 1000 THEN 'Premium'
            WHEN om.lifetime_revenue >= 200  THEN 'Regular'
            ELSE 'New'
        END AS customer_segment,
        CASE
            WHEN rc.days_since_last_order <= 30  THEN 'Active'
            WHEN rc.days_since_last_order <= 90  THEN 'At Risk'
            WHEN rc.days_since_last_order <= 180 THEN 'Lapsing'
            ELSE 'Churned'
        END AS churn_risk
    FROM order_metrics om
    JOIN recency_calc rc USING (user_id)
)

SELECT
    om.user_id,
    om.user_name,
    om.user_email,
    om.user_location,
    om.total_orders,
    ROUND(om.lifetime_revenue::numeric, 2)          AS lifetime_revenue,
    ROUND(om.avg_order_value::numeric, 2)           AS avg_order_value,
    om.first_order_date,
    om.last_order_date,
    rc.days_since_last_order,
    om.distinct_categories,
    om.total_returns,
    om.total_units_purchased,
    seg.customer_segment,
    seg.churn_risk,
    CURRENT_TIMESTAMP                               AS dbt_updated_at
FROM order_metrics om
JOIN recency_calc   rc  USING (user_id)
JOIN segmentation   seg USING (user_id)
