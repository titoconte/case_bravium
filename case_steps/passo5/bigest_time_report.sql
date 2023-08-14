SELECT f.show_id, f.title, MAX(EXTRACT(YEAR FROM date_added) - f.release_year) AS time_interval
FROM Dim_Filmes f
WHERE date_added IS NOT NULL
    AND release_year IS NOT NULL
    AND TYPE='Movie'
GROUP BY f.show_id, f.title
ORDER BY time_interval DESC
LIMIT 1;
