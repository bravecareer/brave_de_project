with

customers as (

    select * from {{ ref('stg_aircanada') }}

),

customer_orders_summary as (

    select
        customers.trading_date,
        min(customers.open_price) as min_open_price,
        max(customers.open_price) as max_open_price,
        avg(customers.open_price) as avg_open_price

    from customers
    group by 1

),

select * from customer_orders_summary
