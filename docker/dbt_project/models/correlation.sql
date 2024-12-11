SELECT 
    CASE 
        -- Define "Good" Weather: No precipitation, no snowfall, and mild temperatures
        WHEN precipitation_sum = 0 AND snowfall_sum = 0 AND temperature_2m_max BETWEEN 10 AND 25 THEN 'Good'

        -- Define "Fair" Weather: Light precipitation or mild snowfall and moderate temperatures
        WHEN precipitation_sum <= 2 AND snowfall_sum <= 2 AND temperature_2m_max BETWEEN 5 AND 30 THEN 'Fair'

        -- Define "Poor" Weather: Moderate precipitation/snowfall or temperatures below freezing
        WHEN precipitation_sum > 2 AND precipitation_sum <= 5 OR snowfall_sum > 2 AND snowfall_sum <= 5 OR temperature_2m_max < 5 THEN 'Poor'

        -- Define "Bad" Weather: Heavy precipitation/snowfall or extreme temperatures
        ELSE 'Bad'
    END AS weather_condition,
    COUNT(*) AS total_accidents
FROM integrated_data
GROUP BY weather_condition
ORDER BY weather_condition

