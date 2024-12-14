{{ config(
    materialized='view',
    schema='staging',
    tags=['staging']
) }}

SELECT
    "date" AS weather_date,
    "temperature_max" AS max_temperature,
    "temperature_min" AS min_temperature,
    "precipitation_sum" AS precipitation,
    "snowfall_sum" AS snowfall,
    "rain_sum" AS rainfall
FROM {{ source('main', 'integrated_data') }}