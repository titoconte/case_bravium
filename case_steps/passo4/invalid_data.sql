-- Relatório de durações inválidas na tabela Dim_Filmes
SELECT show_id, duration
FROM Dim_Filmes
WHERE NOT duration ~ '^[0-9]+( min| Seasons?)?$';

-- Relatório de classificações inválidas na tabela Dim_Filmes
SELECT show_id, rating
FROM Dim_Filmes
WHERE rating NOT IN ('TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA', 'G', 'PG', 'PG-13', 'R', 'NC-17', 'UR');

-- Relatório de show_id inválido na tabela Fato_Filmes
SELECT id, show_id
FROM Fato_Filmes
WHERE show_id NOT IN (SELECT show_id FROM Dim_Filmes);

-- Relatório de actor_id inválido na tabela Fato_Filmes
SELECT id, actor_id
FROM Fato_Filmes
WHERE actor_id NOT IN (SELECT actor_id FROM Dim_Ator);

-- Relatório de listed_in_id inválido na tabela Fato_Filmes
SELECT id, listed_in_id
FROM Fato_Filmes
WHERE listed_in_id NOT IN (SELECT listed_in_id FROM Dim_Genero);

-- Relatório de country_id inválido na tabela Fato_Filmes
SELECT id, country_id
FROM Fato_Filmes
WHERE country_id NOT IN (SELECT country_id FROM Dim_Pais);
