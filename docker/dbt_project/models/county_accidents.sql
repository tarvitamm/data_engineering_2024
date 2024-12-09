WITH bad_weather AS (
    SELECT *,
           CASE 
             WHEN precipitation_sum > 2 OR snowfall_sum > 0 OR temperature_2m_max <= 0 THEN 1 
             ELSE 0 
           END AS is_bad_weather
    FROM integrated_data
)
SELECT 
    Maakond,
    COUNT(*) AS accidents_bad_weather,
    SUM(Vigastatuid) AS total_injured_bad_weather,
    SUM(Hukkunuid) AS total_fatalities_bad_weather
FROM bad_weather
WHERE is_bad_weather = 1
GROUP BY Maakond
ORDER BY accidents_bad_weather DESC
