WITH categorized AS (
    SELECT 
        date,
        CASE
            WHEN temperature_max <= 0 THEN 'Below Freezing (≤0°C)'
            WHEN temperature_max > 0 AND temperature_max <= 10 THEN 'Cold (0-10°C)'
            WHEN temperature_max > 10 AND temperature_max <= 20 THEN 'Mild (10-20°C)'
            WHEN temperature_max > 20 THEN 'Warm (>20°C)'
        END AS temperature_category,
        Vigastatuid,
        Hukkunuid
    FROM integrated_data
)
SELECT
    temperature_category,
    AVG(Vigastatuid) AS avg_injuries_per_accident,
    AVG(Hukkunuid) AS avg_fatalities_per_accident
FROM categorized
GROUP BY temperature_category
ORDER BY temperature_category