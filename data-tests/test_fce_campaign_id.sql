SELECT
    fce.campaign_id
FROM
    {{ ref('fact_campaign_effectiveness_qu') }} fce
LEFT JOIN
    {{ ref('dim_campaign_qu') }} dc
ON
    fce.campaign_id = dc.campaign_id
WHERE
    dc.campaign_id IS NULL