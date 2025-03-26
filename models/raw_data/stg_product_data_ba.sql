SELECT 
    *
FROM
{{ source('de_project', 'product_data') }}