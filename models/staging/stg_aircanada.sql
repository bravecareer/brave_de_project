with

source as (

    select * from {{ source('project_market_risk', 'aircanada_cad') }}

),

renamed as (

    select

        ----------  ids
        date as trading_date,

        ---------- text
        open as open_price

    from source

)

select * from renamed
