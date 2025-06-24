-- Ensure no anomaly values  are in critical columns--

WITH CTE_TEST_1 as (SELECT
    ATCS,
    quantity_sold
FROM {{ref('fact_mkt_engagement_sh_2')}} 
    WHERE ATCS < 0
    OR quantity_sold < 0
),

CTE_TEST_2 as (SELECT
    quantity_in_stock,
  quantity_sold

FROM {{ ref('dim_inventory_sh') }}
    WHERE quantity_in_stock < 0 
    OR quantity_sold < 0
)

SELECT * from CTE_TEST_1 UNION ALL SELECT * from CTE_TEST_2




   