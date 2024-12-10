{{ config(
    materialized='view',
    schema='test_schema'
) }}

SELECT
    "Juhtumi nr" AS accident_id,
    "Toimumisaeg" AS accident_date
FROM {{ source('main', 'integrated_data') }}
LIMIT 5