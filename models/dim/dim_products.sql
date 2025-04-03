select distinct
    product_id,
    'placeholder' as product_category
from {{ ref('stg_users') }}
