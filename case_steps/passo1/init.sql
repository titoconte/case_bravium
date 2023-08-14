CREATE DATABASE netflix_db;
\c netflix_db
CREATE TABLE Dim_Genero (
    listed_in_id SERIAL PRIMARY KEY,
    listed_in VARCHAR(255)
);
CREATE TABLE Dim_Ator (
    actor_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255)
);
CREATE TABLE Dim_Pais (
    country_id SERIAL PRIMARY KEY,
    country VARCHAR(255)
);
CREATE TABLE Dim_Filmes (
    show_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    type VARCHAR(20),
    director VARCHAR(255),
    release_year INTEGER,
    rating VARCHAR(10),
    duration VARCHAR(20),
    date_added DATE,
    description TEXT
);
CREATE TABLE Fato_Filmes (
    id SERIAL PRIMARY KEY,
    show_id INT,
    actor_id INT,
    listed_in_id INT,
    country_id INT,
    FOREIGN KEY (actor_id) REFERENCES Dim_Ator (actor_id),
    FOREIGN KEY (listed_in_id) REFERENCES Dim_Genero (listed_in_id),
    FOREIGN KEY (country_id) REFERENCES Dim_Pais (country_id),
    FOREIGN KEY (show_id) REFERENCES Dim_Filmes (show_id)
);

