-- checking for all combinations of fields are unique
SELECT
    campaign_name,
    medium,
    source,
    content,
    COUNT(*) AS count
FROM
    {{ ref('dim_campaign_qu') }}
GROUP BY
    campaign_name,
    medium,
    source,
    content
HAVING
   count > 1