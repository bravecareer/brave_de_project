select product_id
from {{ ref('fact_campaign_effectiveness_michael_w') }}
where product_id not rlike 'prod-[0-9]{5}'