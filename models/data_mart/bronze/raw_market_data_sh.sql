 ---Extract data for marketing performace metrics---
 SELECT  
    product_id,
    has_atc,
    has_purchase,
    mkt_campaign,
    mkt_content,
    mkt_medium,
    mkt_source,
    timestamp
    FROM {{ source('de_project', 'user_journey') }} uj
    --WHERE uj.has_purchase = TRUE OR uj.has_atc =TRUE 
             --uj.timestamp >= CURRENT_DATE() - 5
    WHERE uj.has_purchase = TRUE
{% if is_incremental() %}
  AND uj.updated_at > COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1990-01-01')
{% endif %}         