WITH tv_years AS (
    SELECT release_year as year, COUNT(*) AS num_tv_shows
    FROM Dim_Filmes 
    WHERE type = 'TV Show'
    GROUP BY release_year
    ORDER BY year
)
SELECT d2.year, 
       100*(d2.num_tv_shows - COALESCE(LAG(d2.num_tv_shows) OVER (ORDER BY d2.year), d2.num_tv_shows)) / COALESCE(LAG(d2.num_tv_shows) OVER (ORDER BY d2.year), 1) AS percent_increase
FROM tv_years d2
ORDER BY percent_increase DESC
LIMIT 1;
