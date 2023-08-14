SELECT EXTRACT(MONTH FROM date_added) AS month, COUNT(*) AS num_releases
FROM Dim_Filmes
GROUP BY month
ORDER BY num_releases DESC
LIMIT 1;
