{{ config(
    materialized='table',
    schema='dimensions',
    tags=['dimensions']
) }}

SELECT DISTINCT
    row_number() OVER (ORDER BY weather_date) AS weather_key,
    weather_date,
    max_temperature,
    min_temperature,
    precipitation,
    snowfall,
    rainfall
FROM {{ ref('staging_weather') }}
