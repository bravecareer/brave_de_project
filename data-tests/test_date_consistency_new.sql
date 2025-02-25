/*
    Test Name: Date Consistency Test
    Description: Validates date-related business rules:
    - Restock date should not be in the future
    - Restock date should not be earlier than last audit date
    - Last audit date should not be in the future
*/

with invalid_dates as (
    select 
        inventory_id,
        restock_date,
        last_audit_date,
        CURRENT_DATE() as current_date
    from {{ ref('stg_inventory_data') }}
    where 
        -- Check for future restock dates
        restock_date > CURRENT_DATE()
        -- Check restock date is not before last audit
        or (restock_date < last_audit_date)
        -- Check for future audit dates
        or last_audit_date > CURRENT_DATE()
)

select * from invalid_dates
