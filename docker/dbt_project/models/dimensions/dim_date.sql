{{ config(
    materialized='table',
    schema='dimensions'
) }}

WITH all_dates AS (
    SELECT CAST(accident_date AS DATE) AS d FROM {{ ref('staging_accidents') }}
    UNION
    SELECT CAST(weather_date AS DATE) AS d FROM {{ ref('staging_weather') }}
)
SELECT
    row_number() OVER (ORDER BY d) AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d::DATE) AS year,
    EXTRACT(MONTH FROM d::DATE) AS month,
    EXTRACT(DAY FROM d::DATE) AS day,
    EXTRACT(DAYOFWEEK FROM d::DATE) AS day_of_week
FROM all_dates
ORDER BY d