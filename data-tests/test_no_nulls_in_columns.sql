-- Ensure no NULL values in critical columns
{{ no_nullS_in_columns(ref('stg_user_journey_BS'))}}
