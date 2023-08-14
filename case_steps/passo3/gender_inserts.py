import json
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
import requests
import unidecode
import asyncio


def CreateMyEngine(fname):

    # Carregar informações de conexão com o banco de dados a partir do arquivo JSON
    with open(fname) as f:
        db_info = json.load(f)

    # Configurar a conexão do SQLAlchemy
    engine = create_engine(f"postgresql+psycopg2://{db_info['user']}:{db_info['password']}@{db_info['host']}:5432/netflix_db")
    
    return engine

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


def InsertsGender(engine):
    # URL da API para obter o gênero dos atores
    # conection 
    Session = sessionmaker(bind=engine)
    session = Session()
    # inserts column
    newcolum_query = text("ALTER TABLE dim_ator ADD COLUMN IF NOT EXISTS gender varchar(255)")
    session.execute(newcolum_query)
    session.commit()
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

if __name__=="__main__":

    # Nome do arquivo com as informações do banco de dados
    dbfname = '../db_infos.json'
    # Criar a engine de conecção do banco através do nome do arquivo json
    engine = CreateMyEngine(dbfname)
    # adicionar genero
    InsertsGender(engine)