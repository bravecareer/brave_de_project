-- Test: Validate user email format
-- Purpose: Ensure emails are not null and contain basic email structure, ignoring header rows.

WITH invalid_emails AS (
    SELECT
        user_id,
        email
    FROM {{ ref('dim_user') }}
    WHERE 
        -- 1. Excluding header or dummy row
        user_id <> 'user_id' 
        AND email <> 'email'
        
        -- 2. check Null
        AND (
            email IS NULL 
            OR email = ''
            -- 3. Basic check for @ symbol
            OR email NOT LIKE '%@%'
        )
)

SELECT * FROM invalid_emails