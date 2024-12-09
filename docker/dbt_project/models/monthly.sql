SELECT 
    strftime(CAST(date AS TIMESTAMP), '%m') AS month,
    AVG((temperature_2m_max + temperature_2m_min) / 2) AS avg_temp, -- Average temperature
    COUNT(*) * 1.0 / COUNT(DISTINCT strftime(CAST(date AS TIMESTAMP), '%Y')) AS avg_accidents -- Average accidents per year
FROM integrated_data
GROUP BY month
ORDER BY month
