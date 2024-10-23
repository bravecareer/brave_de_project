select price, warranty_period_in_month, rating, weight_grams, discount_percentage
from {{ ref('fact_product_performance_michael_w') }}
where price <= 0
OR warranty_period_in_month <= 0
OR rating <= 0
OR weight_grams <= 0
OR discount_percentage < 0