-- Ensure no anomaly values  are in critical columns--
SELECT
  quantity_in_stock,
  quantity_sold

FROM {{ ref('dim_inventory_sh') }}
    WHERE quantity_in_stock < 0 
    OR quantity_sold < 0

SELECT
    ATCS,
    quantity_sold
FROM {{ref('fact_mkt_engagement')}} 
    WHERE ATCS < 0
    OR quantity_sold < 0





   