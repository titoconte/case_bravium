import json
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
import requests
import unidecode
import asyncio


def LoadDBInfo(fname):

    # Carregar informações de conexão com o banco de dados a partir do arquivo JSON
    with open(fname) as f:
        db_info = json.load(f)

    return db_info

def CreateMyEngine(db_info):

    # Configurar a conexão do SQLAlchemy
    engine = create_engine(f"postgresql+psycopg2://{db_info['user']}:{db_info['password']}@{db_info['host']}:5432/netflix_db")
    return engine

def LoadDBBaseFile(fname):
    # Carregar os dados do arquivo CSV 
    df = pd.read_csv(fname)
    return df

def UpNetflixDB(df,engine):
    # Definir os dados das tabelas dimensionais
    dim_filme_data = df[['title', 'type', 'director', 'release_year', 'description','rating','duration','date_added',]]
    dim_filme_data = dim_filme_data.reset_index(drop=True)

    dim_genero_data = df['listed_in'].reset_index(drop=True)
    # Ajustar a tabela de genero já que há generos separados por vírgula e isso prejudicará a análise
    dim_genero_data = dim_genero_data.str.split(', ').explode().drop_duplicates().reset_index(drop=True) 

    dim_pais_data = df['country'].reset_index(drop=True)
    dim_pais_data = dim_pais_data.str.split(', ').explode().drop_duplicates().reset_index(drop=True)
    
    # Ajustar a tabela de atores já uqe há nomes separados por vírgula e isso prejudicará a análise
    dim_ator_data_messy = df['cast'].str.split(', ').explode().fillna('').drop_duplicates().reset_index(drop=True)
    # separar em primeiro nome e ultimos nomes
    dim_ator_data_fname = dim_ator_data_messy.str.split(' ').apply(lambda x: x[0])
    dim_ator_data_lname = dim_ator_data_messy.str.split(' ').apply(lambda x: ''.join(x[1:]))
    dim_ator_data = pd.concat(
        [dim_ator_data_messy,dim_ator_data_fname,dim_ator_data_lname],
        axis=1,
        keys=['full_name','first_name', 'last_name']
        )
    
    dim_genero_data.index+=1
    dim_ator_data.index+=1
    dim_pais_data.index+=1

    # python3 feed_db.py
    # Inserir dados nas tabelas dimensionais definindo index_label e ajustando pra frame qunado necessario
    try:
        dim_filme_data.to_sql('dim_filmes', con=engine, if_exists='append',index_label='show_id')
        print('dados dos filmes já foram inseridos no banco')
    except:
        print('dados dos filmes já foram inseridos anteriormente no banco')
        pass
    try:
        dim_genero_data.to_frame('listed_in').to_sql('dim_genero', con=engine, if_exists='append', index_label='listed_in_id')
        print('dados de genero já foram inseridos no banco')
    except:
        print('dados de genero já foram inseridos anteriormente no banco')
        pass
    try:
        dim_pais_data.to_frame('country').to_sql('dim_pais', con=engine, if_exists='append', index_label='country_id')
        print('dados dos paises já foram inseridos no banco')
    except:
        print('dados dos paises já foram inseridos anteriormente no banco')
        pass
    try:
        dim_ator_data.to_sql('dim_ator', con=engine, if_exists='append', index_label='actor_id')
        print('dados dos atores já foram inseridos no banco')
    except:
        print('dados dos atores já foram inseridos anteriormente no banco')
        pass

    # # Carregar IDs das tabelas dimensionais para fazer o mapeamento
    dim_filmes_ids = pd.read_sql_table('dim_filmes', con=engine).set_index('title')
    dim_genero_ids = pd.read_sql_table('dim_genero', con=engine).set_index('listed_in')
    dim_pais_ids = pd.read_sql_table('dim_pais', con=engine).set_index('country')
    dim_ator_ids = pd.read_sql_table('dim_ator', con=engine).set_index('full_name')

    # Mapear IDs para os dados originais e explodir para as colunas que possuem listas separadas por vírgula
    # também ajustar os textos para os índices
    df['show_id'] = df['title'].map(dim_filmes_ids['show_id'].to_dict())
    df['listed_in'] = df['listed_in'].str.split(', ')
    df = df.explode('listed_in')
    df['listed_in_id'] = df['listed_in'].map(dim_genero_ids['listed_in_id'].to_dict())
    df['country'] = df['country'].str.split(', ')
    df = df.explode('country')
    df['country_id'] = df['country'].map(dim_pais_ids['country_id'].to_dict())
    df['cast'] = df['cast'].str.split(', ')
    df = df.explode('cast')
    df['actor_id'] = df['cast'].map(dim_ator_ids['actor_id'].to_dict())

    # preparar a tabela fato
    df = df[['show_id','actor_id','listed_in_id','country_id']].reset_index(drop=True)

    # Inserir dados na tabela fato
    try:
        df.to_sql('fato_filmes', con=engine, if_exists='append', index_label='id')
    except:
        print('fato filmes inseridos') 
        pass

# função para procesar api assincronamente para depois colocar em escalabilidade
async def GenderApiCall(session,actor_id,full_name_original):
    api_url = "https://innovaapi.aminer.cn/tools/v1/predict/gender?name={}&org=Tsinghua"
    # prepare string
    full_name = unidecode.unidecode(full_name_original)
    full_name = full_name.replace(' ','+')
    # request api
    response = requests.get(api_url.format(full_name))
    if response.status_code == 200:
        gender = response.json()['data']['FGNL']['gender']
    else:
        gender = ' '
    # Atualizar a tabela dim_ator com o gênero
    update_query = text(f"""UPDATE dim_ator SET gender = '{gender}' WHERE actor_id = {actor_id}""")
    session.execute(update_query)
    session.commit()
    print(f'actor gender update for {full_name} ')

def CreateGenderColumn(engine):
    # conection 
    Session = sessionmaker(bind=engine)
    session = Session()
    # inserts column
    newcolum_query = text("ALTER TABLE dim_ator ADD COLUMN IF NOT EXISTS gender varchar(255)")
    session.execute(newcolum_query)
    session.commit()

def InsertsGender(engine):
    # conection 
    Session = sessionmaker(bind=engine)
    session = Session()
    # read actos table
    actors = pd.read_sql_table('dim_ator', con=engine)
    print('tabela atores coletadas')
    # # creates loop event
    loop = asyncio.get_event_loop()
    # # create tasks row
    tasks = [GenderApiCall(session,row['actor_id'],row['full_name']) for _,row  in actors.iterrows()]
    # print(tasks)
    # loop assincrono para upload
    loop.run_until_complete(asyncio.gather(*tasks))
    session.close()

def InvalidData(engine):
    # Carregar tabelas
    dim_filmes = pd.read_sql_table('dim_filmes', con=engine)
    dim_genero = pd.read_sql_table('dim_genero', con=engine)
    dim_pais = pd.read_sql_table('dim_pais', con=engine)
    dim_ator = pd.read_sql_table('dim_ator', con=engine)
    fato_filmes = pd.read_sql_table('fato_filmes', con=engine)
    # Relatório de durações inválidas na tabela Dim_Filmes
    invalid_durations = dim_filmes[~dim_filmes['duration'].str.match(r'^[0-9]+( min| Seasons?)?$')]
    print(invalid_durations[['show_id', 'duration']])

    # Relatório de classificações inválidas na tabela Dim_Filmes
    invalid_ratings = dim_filmes[~dim_filmes['rating'].isin(['TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA', 'G', 'PG', 'PG-13', 'R', 'NC-17', 'UR'])]
    print(invalid_ratings[['show_id', 'rating']])

    # Relatório de show_id inválido na tabela Fato_Filmes
    invalid_show_ids = fato_filmes[~fato_filmes['show_id'].isin(dim_filmes['show_id'])]
    print(invalid_show_ids[['id', 'show_id']])

    # Relatório de actor_id inválido na tabela Fato_Filmes
    invalid_actor_ids = fato_filmes[~fato_filmes['actor_id'].isin(dim_ator['actor_id'])]
    print(invalid_actor_ids[['id', 'actor_id']])

    # Relatório de listed_in_id inválido na tabela Fato_Filmes
    invalid_listed_in_ids = fato_filmes[~fato_filmes['listed_in_id'].isin(dim_genero['listed_in_id'])]
    print(invalid_listed_in_ids[['id', 'listed_in_id']])

    # Relatório de country_id inválido na tabela Fato_Filmes
    invalid_country_ids = fato_filmes[~fato_filmes['country_id'].isin(dim_pais['country_id'])]
    print(invalid_country_ids[['id', 'country_id']])


def MissingData(engine):
    # Carregar tabelas
    dim_filmes = pd.read_sql_table('dim_filmes', con=engine)
    dim_genero = pd.read_sql_table('dim_genero', con=engine)
    dim_pais = pd.read_sql_table('dim_pais', con=engine)
    dim_ator = pd.read_sql_table('dim_ator', con=engine)
    fato_filmes = pd.read_sql_table('fato_filmes', con=engine)
    # Relatório de dados ausentes na tabela Dim_Ator
    missing_actors = dim_ator[(dim_ator['full_name'].isnull()) | (dim_ator['first_name'].isnull()) | (dim_ator['last_name'].isnull())]
    print(missing_actors[['actor_id', 'full_name']])

    # Relatório de dados ausentes na tabela Dim_Pais
    missing_countries = dim_pais[dim_pais['country'].isnull()]
    print(missing_countries[['country_id', 'country']])

    # Relatório de dados ausentes na tabela Dim_Genero
    missing_genres = dim_genero[dim_genero['listed_in'].isnull()]
    print(missing_genres[['listed_in_id', 'listed_in']])

    # Relatório de dados ausentes na tabela Dim_Filmes
    missing_films = dim_filmes[(dim_filmes['title'].isnull()) | (dim_filmes['release_year'].isnull()) |
                            (dim_filmes['rating'].isnull()) | (dim_filmes['duration'].isnull()) |
                            (dim_filmes['date_added'].isnull())]
    print(missing_films[['show_id', 'title']])

    # Relatório de dados ausentes na tabela Fato_Filmes
    missing_fact_films = fato_filmes[(fato_filmes['show_id'].isnull()) | (fato_filmes['actor_id'].isnull()) |
                                    (fato_filmes['listed_in_id'].isnull()) | (fato_filmes['country_id'].isnull())]
    print(missing_fact_films[['id', 'show_id', 'actor_id', 'listed_in_id', 'country_id']])

    

def Qustions(engine):
    # Carregar tabelas
    dim_filmes = pd.read_sql_table('dim_filmes', con=engine)
    dim_ator = pd.read_sql_table('dim_ator', con=engine)
    fato_filmes = pd.read_sql_table('fato_filmes', con=engine)

    # Consulta para encontrar o primeiro nome mais comum entre atores e atrizes
    most_common_first_name = dim_ator['first_name'].value_counts().idxmax()
    print("Primeiro nome mais comum:", most_common_first_name)

    # Consulta para encontrar o filme com o maior intervalo de tempo desde o lançamento até aparecer na Netflix
    movie_time_interval = dim_filmes[(dim_filmes['date_added'].notnull()) &
                                    (dim_filmes['release_year'].notnull()) &
                                    (dim_filmes['type'] == 'Movie')]
    movie_time_interval['time_interval'] = (pd.to_datetime(movie_time_interval['date_added']).dt.year -
                                            movie_time_interval['release_year'])
    movie_time_interval = movie_time_interval.sort_values(by='time_interval', ascending=False).iloc[0]
    print("Filme com o maior intervalo:", movie_time_interval['title'])
    print("Maior intervalo:", movie_time_interval['time_interval'], "anos")

    # Consulta para encontrar o mês com mais lançamentos historicamente
    most_common_month = dim_filmes['date_added'].dt.month.value_counts().idxmax()
    print("Mês com mais lançamentos:", most_common_month)

    woody_actors = dim_ator[(dim_ator['first_name'] == 'Woody') & (dim_ator['last_name'] == 'Harrelson')]
    woody_actor_id = woody_actors['actor_id'].values[0]

    actress_list = []
    for _, row in fato_filmes.iterrows():
        if row['actor_id'] == woody_actor_id:
            show_id = row['show_id']
            common_actors = fato_filmes[fato_filmes['show_id'] == show_id]
            actress_actors = common_actors[common_actors['actor_id'] != woody_actor_id]
            actress_actors = actress_actors.merge(dim_ator, on='actor_id')
            actress_actors = actress_actors[actress_actors['gender'] == 'female']
            
            if len(actress_actors) > 1:
                actress_list.append(actress_actors[['first_name', 'last_name']])
                
    actress_list = pd.concat(actress_list).drop_duplicates()
    print("Atrizes que apareceram em um filme com Woody Harrelson mais de uma vez:")
    print(actress_list)

    # Consulta para encontrar o ano com o maior aumento percentual ano a ano para programas de TV
    tv_years = dim_filmes[(dim_filmes['type'] == 'TV Show') & (dim_filmes['release_year'].notnull())]
    tv_years_grouped = tv_years.groupby('release_year').size().reset_index(name='num_tv_shows').sort_values(by='release_year')
    tv_years_grouped['percent_increase'] = 100 * (tv_years_grouped['num_tv_shows'] - tv_years_grouped['num_tv_shows'].shift(1, fill_value=tv_years_grouped['num_tv_shows'][0])) / tv_years_grouped['num_tv_shows'].shift(1, fill_value=tv_years_grouped['num_tv_shows'][0])
    tv_years_grouped = tv_years_grouped.sort_values(by='percent_increase', ascending=False).iloc[0]
    print("Ano com o maior aumento percentual ano a ano para programas de TV:", int(tv_years_grouped['release_year']))
    print("Maior aumento percentual:", round(tv_years_grouped['percent_increase'], 2), "%")


if __name__=="__main__":

    # Nome do arquivo com as informações do banco de dados
    dbfname = '../db_infos.json'
    # Nome do banco de dados inicial para upload
    fname = 'netflix_titles.csv'
    # carregar o arquivo
    db_info = LoadDBInfo(fname)
    # Criar a engine de conecção do banco através do nome do arquivo json
    engine = CreateMyEngine(db_info)
    # ler arquivo de entrada
    df = LoadDBBaseFile(fname)
    # Subir dados do nome dos arquivos
    UpNetflixDB(df,engine)
    # criar coluna de gênero
    CreateGenderColumn(engine)
    # adicionar genero
    InsertsGender(engine)
    # reports
    Qustions(engine)
    InvalidData(engine)
    MissingData(engine)
