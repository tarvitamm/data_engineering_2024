{{ config(
    materialized='table',
    schema='dimensions'
) }}

SELECT DISTINCT
    row_number() OVER (ORDER BY region, municipality, settlement, x_coordinate, y_coordinate) AS location_key,
    region,
    municipality,
    settlement,
    x_coordinate,
    y_coordinate
FROM {{ ref('staging_accidents') }}