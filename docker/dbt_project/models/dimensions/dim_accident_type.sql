{{ config(
    materialized='table',
    schema='dimensions',
    tags=['dimensions']
) }}

SELECT DISTINCT
    row_number() OVER (ORDER BY accident_type, accident_detail, drunk_driver_involved) AS accident_type_key,
    accident_type,
    accident_detail,
    CASE
        WHEN drunk_driver_involved = 1.0 THEN TRUE
        WHEN drunk_driver_involved = 0.0 THEN FALSE
        ELSE NULL  -- Handle unexpected values (optional)
    END AS drunk_driver_involved
FROM {{ ref('staging_accidents') }}
