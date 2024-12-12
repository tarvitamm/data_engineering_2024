WITH time_weather_analysis AS (
    SELECT
        CASE
            WHEN EXTRACT(HOUR FROM CAST(Toimumisaeg AS TIMESTAMP)) BETWEEN 6 AND 12 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM CAST(Toimumisaeg AS TIMESTAMP)) BETWEEN 12 AND 18 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM CAST(Toimumisaeg AS TIMESTAMP)) BETWEEN 18 AND 24 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        precipitation_sum, -- Replace with actual weather column name
        COUNT(*) AS total_accidents
    FROM
        integrated_data
    GROUP BY
        precipitation_sum, time_of_day
)
SELECT
    precipitation_sum AS weather_condition, -- Replace with actual weather column name
    time_of_day,
    total_accidents
FROM
    time_weather_analysis
ORDER BY
    total_accidents DESC
LIMIT 1
