SELECT FIRST_NAME, COUNT(*) AS frequency
FROM dim_ator
GROUP BY FIRST_NAME
ORDER BY frequency DESC
LIMIT 1;
