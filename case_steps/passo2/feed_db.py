import json
import pandas as pd
from sqlalchemy import create_engine,text


def CreateMyEngine(fname):

    # Carregar informações de conexão com o banco de dados a partir do arquivo JSON
    with open(fname) as f:
        db_info = json.load(f)

    # Configurar a conexão do SQLAlchemy
    engine = create_engine(f"postgresql+psycopg2://{db_info['user']}:{db_info['password']}@{db_info['host']}:5432/netflix_db")
    
    return engine

def UpNetflixDB(fname,engine):
    # Carregar os dados do arquivo CSV 
    df = pd.read_csv(fname)

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
    
if __name__=="__main__":

    # Nome do arquivo com as informações do banco de dados
    dbfname = '../db_infos.json'
    # Nome do banco de dados inicial para upload
    fname = 'netflix_titles.csv'
    # Criar a engine de conecção do banco através do nome do arquivo json
    engine = CreateMyEngine(dbfname)
    # Subir dados do nome dos arquivos
    UpNetflixDB(fname,engine)

