-- Staging model for fact_orders
-- Data Lineage: fact_orders (PostgreSQL) -> staging/stg_orders.sql

{{ config(materialized='view', schema='staging') }}

SELECT
    fo.order_id,
    fo.order_sk,
    du.user_id,
    du.name                                 AS user_name,
    du.email                                AS user_email,
    du.location                             AS user_location,
    dp.product_id,
    dp.name                                 AS product_name,
    dp.category,
    dp.price                                AS list_price,
    dd.full_date                            AS order_date,
    dd.year,
    dd.quarter,
    dd.month,
    dd.week_of_year,
    dd.day_of_week,
    dd.is_weekend,
    fo.quantity,
    fo.unit_price,
    fo.total_amount,
    fo.status,
    fo.order_time
FROM {{ source('ecommerce', 'fact_orders') }}      fo
JOIN {{ source('ecommerce', 'dim_users') }}         du ON fo.user_sk    = du.user_sk
JOIN {{ source('ecommerce', 'dim_products') }}      dp ON fo.product_sk = dp.product_sk
JOIN {{ source('ecommerce', 'dim_date') }}          dd ON fo.date_sk    = dd.date_sk
