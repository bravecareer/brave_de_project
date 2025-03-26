SELECT 
    *
FROM
{{ source('de_project', 'inventory_data') }}