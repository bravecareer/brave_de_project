{{ config(
   materialized='table',
   unique_key='cart_id'
) }}

-- depends_on: {{ ref('stg_user_journey_ba') }}

WITH cart AS (
    SELECT DISTINCT
        c.cart_id,
        c.selected_store_id,
        c.selected_timeslot_date,
        c.selected_timeslot_time,
        c.selected_timeslot_type,
        c.fulfillment_type,
        c.shopping_mode
FROM {{ ref('stg_user_journey_ba') }} c
WHERE cart_id IS NOT NULL
)

SELECT * FROM cart