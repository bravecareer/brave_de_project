select price, quantity_sold, total_sales_amount
from {{ ref('fact_product_purchase_michael_w') }}
where price * quantity_sold != total_sales_amount