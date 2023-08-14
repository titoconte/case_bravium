# PostgreSQL
FROM postgres:14
# Criar o DB
COPY ./case_steps/passo1/init.sql /docker-entrypoint-initdb.d/

# Dependencias
RUN apt-get update && apt-get install -y python3 python3-pip  libpq-dev build-essential
RUN apt-get update && apt-get install vim -y

# definir workdir
WORKDIR /case_steps
# requirements
COPY requirements.txt ./

# Instala pacotes
RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED
RUN pip install -r requirements.txt

# Define a variável de ambiente para evitar diálogos de instalação do psycopg2
ENV POSTGRES_PASSWORD=batatachips




