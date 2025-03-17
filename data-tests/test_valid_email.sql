WITH invalid_emails AS (
    SELECT
        user_id,
        email
    FROM {{ ref('dim_user_sa') }}
    WHERE email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
)
SELECT COUNT(*) AS failures 
FROM invalid_emails;
