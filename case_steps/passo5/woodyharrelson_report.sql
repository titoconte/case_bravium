SELECT da1.first_name, da1.last_name
FROM Dim_Ator da1
JOIN Fato_Filmes ff1 ON da1.actor_id = ff1.actor_id
JOIN Fato_Filmes ff2 ON ff1.show_id = ff2.show_id
JOIN Dim_Ator da2 ON ff2.actor_id = da2.actor_id AND da2.first_name = 'Woody' AND da2.last_name = 'Harrelson'
WHERE da1.first_name != 'Woody' AND da1.last_name != 'Harrelson' AND da2.gender='female'
GROUP BY da1.first_name, da1.last_name
HAVING COUNT(DISTINCT ff1.show_id) > 1;
