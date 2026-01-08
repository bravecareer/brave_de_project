{{ config(
   materialized='incremental',
   unique_key=['campaign_date', 'mkt_campaign', 'product_id']
) }}

WITH user_journey_with_marketing AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.timestamp,
        uj.session_id,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.mkt_source,
        uj.mkt_medium,
        uj.mkt_campaign,
        uj.mkt_content
    FROM {{ source('de_project', 'user_journey') }} uj
    WHERE uj.mkt_campaign IS NOT NULL
),

campaign_engagement AS (
    SELECT
        DATE(timestamp) AS campaign_date,
        mkt_campaign,
        mkt_source,
        mkt_medium,
        product_id,
        
        -- User counts
        COUNT(DISTINCT user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN has_qv = TRUE THEN user_id END) AS qv_users,
        COUNT(DISTINCT CASE WHEN has_pdp = TRUE THEN user_id END) AS pdp_users,
        COUNT(DISTINCT CASE WHEN has_atc = TRUE THEN user_id END) AS atc_users,
        COUNT(DISTINCT CASE WHEN has_purchase = TRUE THEN user_id END) AS purchase_users,
        
        -- Event counts
        SUM(CASE WHEN has_qv = TRUE THEN 1 ELSE 0 END) AS total_qv,
        SUM(CASE WHEN has_pdp = TRUE THEN 1 ELSE 0 END) AS total_pdp,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) AS total_atc,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) AS total_purchases
        
    FROM user_journey_with_marketing
    GROUP BY DATE(timestamp), mkt_campaign, mkt_source, mkt_medium, product_id
),

campaign_revenue AS (
    SELECT
        ce.campaign_date,
        ce.mkt_campaign,
        ce.mkt_source,
        ce.mkt_medium,
        ce.product_id,
        ce.total_users,
        ce.qv_users,
        ce.pdp_users,
        ce.atc_users,
        ce.purchase_users,
        ce.total_qv,
        ce.total_pdp,
        ce.total_atc,
        ce.total_purchases,
        
        -- Revenue calculation
        COALESCE(SUM(t.total_amount), 0) AS total_revenue,
        COALESCE(AVG(t.total_amount), 0) AS avg_order_value
        
    FROM campaign_engagement ce
    LEFT JOIN {{ ref('fact_user_transaction') }} t
        ON ce.product_id = t.product_id
        AND DATE(ce.campaign_date) = DATE(t.timestamp)
    GROUP BY 
        ce.campaign_date,
        ce.mkt_campaign,
        ce.mkt_source,
        ce.mkt_medium,
        ce.product_id,
        ce.total_users,
        ce.qv_users,
        ce.pdp_users,
        ce.atc_users,
        ce.purchase_users,
        ce.total_qv,
        ce.total_pdp,
        ce.total_atc,
        ce.total_purchases
),

campaign_metrics AS (
    SELECT
        campaign_date,
        mkt_campaign,
        mkt_source,
        mkt_medium,
        product_id,
        total_users,
        qv_users,
        pdp_users,
        atc_users,
        purchase_users,
        total_qv,
        total_pdp,
        total_atc,
        total_purchases,
        total_revenue,
        avg_order_value,
        
        -- Conversion rates
        CASE WHEN total_users > 0 
            THEN ROUND((atc_users::FLOAT / total_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS campaign_atc_rate,
        
        CASE WHEN total_users > 0 
            THEN ROUND((purchase_users::FLOAT / total_users::FLOAT) * 100, 2)
            ELSE 0 
        END AS campaign_conversion_rate,
        
        -- Cost per metrics (assuming $1 per click as placeholder)
        ROUND(total_users * 1.0, 2) AS estimated_campaign_cost,
        
        CASE WHEN total_users > 0
            THEN ROUND(total_revenue / total_users, 2)
            ELSE 0
        END AS revenue_per_user
        
    FROM campaign_revenue
),

final AS (
    SELECT
        cm.*,
        p.product_name,
        p.product_category,
        
        -- ROI calculation
        CASE WHEN cm.estimated_campaign_cost > 0
            THEN ROUND(((cm.total_revenue - cm.estimated_campaign_cost) / cm.estimated_campaign_cost) * 100, 2)
            ELSE 0
        END AS campaign_roi_percentage
        
    FROM campaign_metrics cm
    LEFT JOIN {{ ref('dim_product') }} p
        ON cm.product_id = p.product_id
)

SELECT * FROM final
