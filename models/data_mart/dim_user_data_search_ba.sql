/*UserID	Primary key
FirstName, LastName, Email	
SignupDate, DOB, PreferredLanguage	
MarketingOptIn, AccountStatus	
LoyaltyPointsBalance*/

{{ config(
   materialized='table',
   unique_key='user_id'
) }}

WITH users AS ( 
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.signup_date,
        u.preferred_language,
        u.dob,
        u.marketing_opt_in,
        u.account_status,
        u.loyalty_points_balance
FROM {{ ref('stg_user_data_ba') }} u
)

SELECT * FROM users