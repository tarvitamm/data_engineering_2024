{{ config(
    materialized='table',
    schema='fact'
) }}

WITH base AS (
    SELECT DISTINCT
        sa.accident_id,
        sa.accident_date,
        sa.num_people_involved,
        sa.num_deaths,
        sa.num_vehicles_involved,
        sa.num_injuries,
        sa.region,
        sa.municipality,
        sa.settlement,
        sa.x_coordinate,
        sa.y_coordinate,
        sa.accident_type,
        sa.accident_detail,
        sa.drunk_driver_involved,
        sw.weather_date
    FROM {{ ref('staging_accidents') }} sa
    LEFT JOIN {{ ref('staging_weather') }} sw ON sa.accident_date = sw.weather_date
),

date_mapping AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY full_date) AS date_key, -- Dynamically assign keys
        full_date
    FROM (
        SELECT DISTINCT full_date 
        FROM {{ ref('dim_date') }}
    ) unique_dates
),

location_mapping AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY region, municipality, settlement, x_coordinate, y_coordinate) AS location_key, -- Dynamically assign keys
        region, 
        municipality, 
        settlement, 
        x_coordinate, 
        y_coordinate
    FROM (
        SELECT DISTINCT 
            region, 
            municipality, 
            settlement, 
            x_coordinate, 
            y_coordinate
        FROM {{ ref('dim_location') }}
    ) unique_locations
),

accident_type_mapping AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY accident_type, accident_detail, drunk_driver_involved) AS accident_type_key, -- Dynamically assign keys
        accident_type, 
        accident_detail, 
        drunk_driver_involved
    FROM (
        SELECT DISTINCT 
            accident_type, 
            accident_detail, 
            drunk_driver_involved
        FROM {{ ref('dim_accident_type') }}
    ) unique_combinations
),

weather_mapping AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY weather_date) AS weather_key, -- Dynamically assign keys
        weather_date
    FROM (
        SELECT DISTINCT weather_date 
        FROM {{ ref('dim_weather') }}
    ) unique_weather
)


SELECT
    b.accident_id,
    d.date_key,
    l.location_key,
    a.accident_type_key,
    w.weather_key,
    b.num_people_involved,
    b.num_deaths,
    b.num_vehicles_involved,
    b.num_injuries
FROM base b
LEFT JOIN date_mapping d ON b.accident_date = d.full_date
LEFT JOIN location_mapping l ON b.region = l.region
    AND b.municipality = l.municipality
    AND b.settlement = l.settlement
    AND b.x_coordinate = l.x_coordinate
    AND b.y_coordinate = l.y_coordinate 
LEFT JOIN accident_type_mapping a 
    ON b.accident_type = a.accident_type
    AND b.accident_detail = a.accident_detail
    AND b.drunk_driver_involved = a.drunk_driver_involved
LEFT JOIN weather_mapping w ON b.weather_date = w.weather_date