{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

WITH staged_user_data AS (
    SELECT *
    FROM {{ ref('stg_user_data_nb') }}
    WHERE account_status = 'active'
)

SELECT * FROM staged_user_data