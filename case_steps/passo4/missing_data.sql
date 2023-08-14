-- Relatório de dados ausentes na tabela Dim_Ator
SELECT actor_id, full_name
FROM Dim_Ator
WHERE full_name IS NULL first_name IS NULL OR last_name IS NULL;

-- Relatório de dados ausentes na tabela Dim_Pais
SELECT country_id, country
FROM Dim_Pais
WHERE country IS NULL;

-- Relatório de dados ausentes na tabela Dim_Genero
SELECT listed_in_id, listed_in
FROM Dim_Genero
WHERE listed_in IS NULL;

-- Relatório de dados ausentes na tabela Dim_Filmes
SELECT show_id, title
FROM Dim_Filmes
WHERE title IS NULL OR release_year IS NULL OR rating IS NULL OR duration IS NULL OR date_added IS NULL;

-- Relatório de dados ausentes na tabela Fato_Filmes
SELECT id, show_id, actor_id, listed_in_id, country_id
FROM Fato_Filmes
WHERE show_id IS NULL OR actor_id IS NULL OR listed_in_id IS NULL OR country_id IS NULL;
