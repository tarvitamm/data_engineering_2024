WITH filtered_data AS (
    SELECT
        Maakond,
        "Liiklusõnnetuse liik",
        COUNT(*) AS total_accidents
    FROM
        integrated_data
    GROUP BY
        Maakond, "Liiklusõnnetuse liik"
)
SELECT *
FROM filtered_data
ORDER BY total_accidents DESC
