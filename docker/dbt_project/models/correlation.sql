WITH weather_analysis AS (
    SELECT
        CASE 
            WHEN precipitation_sum > 0 THEN 'Bad Weather'
            ELSE 'Good Weather'
        END AS weather_condition,
        COUNT(*) AS total_accidents
    FROM
        integrated_data
    GROUP BY
        weather_condition
)
SELECT *
FROM weather_analysis
