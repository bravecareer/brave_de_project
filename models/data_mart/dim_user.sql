WITH user_data AS (
   SELECT
       user_id,
       email,
       loyalty_points_balance,
       account_status,
       signup_date,
       first_name,
       last_name,
       CASE 
           WHEN loyalty_points_balance >= 1000 THEN 'Gold'
           WHEN loyalty_points_balance BETWEEN 500 AND 999 THEN 'Silver'
           ELSE 'Regular'
       END AS loyalty_tier,
       DATEDIFF(day, signup_date, CURRENT_DATE()) AS account_age_days,
       preferred_contact_method,
       marketing_opt_in,
       dob,
       CASE WHEN loyalty_points_balance >= 1000 THEN TRUE ELSE FALSE END AS is_loyal_customer
   FROM {{ source('de_project', 'user_data') }} u
   WHERE account_status = 'active'
     AND email IS NOT NULL  -- always good to explicitly filter valid emails here
)
SELECT * FROM user_data
