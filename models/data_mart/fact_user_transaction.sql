-- fact_user_transaction.sql: Incremental table capturing recent user transactions
{{ config(
    materialized='incremental',
    unique_key=['user_id', 'search_event_id', 'product_id', 'transaction_timestamp']
) }}

-- Identify recent confirmed purchases within the last 5 days
WITH recent_purchases AS (
    SELECT
        uj.user_id,                                              -- User ID associated with transaction
        uj.product_id,                                           -- Purchased product ID
        uj.search_event_id,                                      -- Search event ID linked to transaction
        uj.session_id,                                           -- Session ID of transaction
        uj.cart_id,                                              -- Shopping cart identifier
        TRY_TO_TIMESTAMP(REPLACE(uj.timestamp, ' UTC', '')) AS transaction_timestamp -- Parsed transaction timestamp
    FROM {{ source('de_project', 'user_journey') }} uj
    WHERE uj.has_purchase = TRUE
      AND TRY_TO_TIMESTAMP(REPLACE(uj.timestamp, ' UTC', '')) >= DATEADD(day, -5, CURRENT_TIMESTAMP())
),

-- Filter active users for transaction validation
valid_users AS (
    SELECT user_id
    FROM {{ source('de_project', 'user_data') }}
    WHERE account_status = 'active'
),

-- Validate products and retrieve pricing
valid_products AS (
    SELECT product_id, price
    FROM {{ source('de_project', 'product_data') }}
),

-- Aggregate transactions ensuring valid user and product references
transaction_data AS (
    SELECT
        rp.user_id,
        rp.product_id,
        rp.search_event_id,
        rp.session_id,
        rp.cart_id,
        rp.transaction_timestamp,
        COUNT(*) AS quantity_sold,                               -- Total quantity purchased
        SUM(vp.price) AS total_amount                            -- Total transaction amount
    FROM recent_purchases rp
    INNER JOIN valid_users vu ON rp.user_id = vu.user_id         -- Join valid active users
    INNER JOIN valid_products vp ON rp.product_id = vp.product_id-- Join valid product/pricing details
    GROUP BY
        rp.user_id,
        rp.product_id,
        rp.search_event_id,
        rp.session_id,
        rp.cart_id,
        rp.transaction_timestamp
)

-- Explicit final column selection
SELECT
    user_id,
    product_id,
    search_event_id,
    session_id,
    cart_id,
    transaction_timestamp,
    quantity_sold,
    total_amount
FROM transaction_data
