WITH time_weather AS (
    SELECT
        CASE 
            WHEN CAST(strftime(CAST(time AS TIMESTAMP), '%H') AS INTEGER) < 6 THEN 'Night (00:00-05:59)'
            WHEN CAST(strftime(CAST(time AS TIMESTAMP), '%H') AS INTEGER) < 12 THEN 'Morning (06:00-11:59)'
            WHEN CAST(strftime(CAST(time AS TIMESTAMP), '%H') AS INTEGER) < 18 THEN 'Afternoon (12:00-17:59)'
            ELSE 'Evening (18:00-23:59)'
        END AS time_of_day,

        CASE 
            WHEN precipitation_sum = 0 THEN 'No precipitation'
            WHEN precipitation_sum <= 2 THEN 'Light precipitation'
            WHEN precipitation_sum <= 5 THEN 'Moderate precipitation'
            ELSE 'Heavy precipitation'
        END AS precipitation_category
    FROM integrated_data
    WHERE time IS NOT NULL AND CAST(time AS TIMESTAMP) IS NOT NULL -- Exclude invalid rows
)
SELECT time_of_day, precipitation_category, COUNT(*) AS total_accidents
FROM time_weather
GROUP BY time_of_day, precipitation_category
ORDER BY time_of_day, precipitation_category
