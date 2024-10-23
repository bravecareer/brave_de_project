{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

With total_purchases as(
    select user_id, sum(CASE WHEN has_purchase THEN 1 ELSE 0 END) as total_orders 
    from {{ ref('stg_userjourney_sp') }} 
    group by user_id
),

total_amount as(
select u.user_id, sum(p.price) as total_amount 
from {{ ref('stg_userjourney_sp') }} u left join {{ ref('stg_products_sp') }} p on u.product_id = p.product_id group by user_id
),

user_summary as (

select user_id, max(date_last_purchase) as date_last_purchase, max(lifetime_offline_orders_count) as lifetime_offline_orders_count, max(lifetime_online_orders_count) as lifetime_online_orders_count
from {{ ref('stg_userjourney_sp') }}
group by user_id
),

final as (

select u.user_id, t1.total_orders, t2.total_amount, u.loyalty_points_balance, u2.date_last_purchase, u2.lifetime_offline_orders_count, u2.lifetime_online_orders_count
    from {{ ref('stg_userdata_sp') }} u left join total_purchases t1 on u.user_id = t1.user_id
                     left join total_amount t2 on u.user_id = t2.user_id
                     left join user_summary u2 on u.user_id = u2.user_id

    having u.account_status = 'active'

)

select * from final



