-- Test for invalid emails in the cleaned_user_data
WITH invalid_emails AS (
  SELECT
    user_id,
    email
  FROM {{ ref('DIM_USER_JC') }}
  WHERE email NOT LIKE '%_@__%.__%'
)
SELECT * FROM invalid_emails