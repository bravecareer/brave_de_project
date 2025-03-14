-- Test for NULL user_ids in the cleaned_user_data
WITH null_user_ids AS (
  SELECT
    user_id
  FROM {{ ref('DIM_USER_JC') }}
  WHERE user_id IS NULL
)
SELECT * FROM null_user_ids