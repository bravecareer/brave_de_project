SELECT DISTINCT mkt_medium
FROM {{ ref('dim_campaign') }}
WHERE mkt_medium NOT IN ('paid search', 'email', 'social', 'display')
