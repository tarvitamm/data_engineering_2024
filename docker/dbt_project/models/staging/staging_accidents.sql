{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    "Juhtumi nr" AS accident_id,
    "Toimumisaeg" AS accident_date,
    "Isikuid" AS num_people_involved,
    "Hukkunuid" AS num_deaths,
    "Sõidukeid" AS num_vehicles_involved,
    "Vigastatuid" AS num_injuries,
    "Maakond" AS region,
    "Omavalitsus" AS municipality,
    "Asula" AS settlement,
    "Liiklusõnnetuse liik" AS accident_type,
    "Liiklusõnnetuse liik (detailne)" AS accident_detail,
    "Joobes mootorsõidukijuhi osalusel" AS drunk_driver_involved,
    "X koordinaat" AS x_coordinate,
    "Y koordinaat" AS y_coordinate,
FROM integrated_data