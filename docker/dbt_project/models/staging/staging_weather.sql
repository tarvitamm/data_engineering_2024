{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    "date" AS weather_date,
    "temperature_2m_max" AS max_temperature,
    "temperature_2m_min" AS min_temperature,
    "precipitation_sum" AS precipitation,
    "snowfall_sum" AS snowfall,
    "rain_sum" AS rainfall,
FROM {{ source('main', 'integrated_data') }}