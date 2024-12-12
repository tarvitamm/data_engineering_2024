WITH bad_weather_accidents AS (
    SELECT
        Maakond,
        COUNT(*) AS total_accidents
    FROM
        integrated_data
    WHERE
        precipitation_sum > 0
    GROUP BY
        Maakond
)
SELECT Maakond, total_accidents
FROM bad_weather_accidents
ORDER BY total_accidents DESC
LIMIT 1
