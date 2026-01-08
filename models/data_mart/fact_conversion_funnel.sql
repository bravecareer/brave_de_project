{{ config(
   materialized='incremental',
   unique_key=['funnel_date', 'product_id']
) }}

WITH daily_funnel AS (
    SELECT
        DATE(timestamp) AS funnel_date,
        product_id,
        COUNT(DISTINCT user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN has_qv = TRUE THEN user_id END) AS qv_users,
        COUNT(DISTINCT CASE WHEN has_pdp = TRUE THEN user_id END) AS pdp_users,
        COUNT(DISTINCT CASE WHEN has_atc = TRUE THEN user_id END) AS atc_users,
        COUNT(DISTINCT CASE WHEN has_purchase = TRUE THEN user_id END) AS purchase_users,
        
        -- Event counts
        SUM(CASE WHEN has_qv = TRUE THEN 1 ELSE 0 END) AS qv_events,
        SUM(CASE WHEN has_pdp = TRUE THEN 1 ELSE 0 END) AS pdp_events,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) AS atc_events,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) AS purchase_events
    FROM {{ ref('fact_user_engagement') }}
    GROUP BY DATE(timestamp), product_id
),

conversion_metrics AS (
    SELECT
        funnel_date,
        product_id,
        total_users,
        qv_users,
        pdp_users,
        atc_users,
        purchase_users,
        qv_events,
        pdp_events,
        atc_events,
        purchase_events,
        
        -- Conversion rates (as percentages)
        CASE WHEN qv_users > 0 
            THEN ROUND((pdp_users::FLOAT / qv_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS qv_to_pdp_rate,
        
        CASE WHEN pdp_users > 0 
            THEN ROUND((atc_users::FLOAT / pdp_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS pdp_to_atc_rate,
        
        CASE WHEN atc_users > 0 
            THEN ROUND((purchase_users::FLOAT / atc_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS atc_to_purchase_rate,
        
        CASE WHEN qv_users > 0 
            THEN ROUND((purchase_users::FLOAT / qv_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS overall_conversion_rate
        
    FROM daily_funnel
),

final AS (
    SELECT
        cf.*,
        p.product_name,
        p.product_category
    FROM conversion_metrics cf
    LEFT JOIN {{ ref('dim_product') }} p
        ON cf.product_id = p.product_id
)

SELECT * FROM final
