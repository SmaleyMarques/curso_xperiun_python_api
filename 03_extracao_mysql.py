# %%
# SCRIPT PARA EXTRAÇÃO DE DADOS DA API DA XFINANCE E INPUT NO MYSQL

import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time

# Instanciando arquivo .env para acessar TOKEN
load_dotenv()

# Declarando Token para Autenticação
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

url_transactions = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"

# Definindo Função para conexão e extração de dados na API com paginação
def chamar_api_xfinance(url):

    cursor = 0 # iniciando cursor em 0
    dados = [] # criando tabela para receber dados de resposta

    # Loop para paginação
    while True:

        # Fazendo requisição
        response = requests.get(url, headers=headers, params={'cursor': cursor})
        response_json = response.json().get('response', None)

        # Verificando se requisição não esta retornado vazia
        if response_json is not None:
            
            results = response_json.get('results', [])

            remaining = response_json.get('remaining', 0)

            # Extendendo Lista de dados 
            dados.extend(results)

            # Verificando se ainda existe dados restantes
            if remaining <= 0:
                break
        else:
            break

        # Incremetando cursor para próxima página
        cursor += 100
        # Pausa de 1 segundo no código
        time.sleep(1)
    
    return dados

# Chamando função
transactions_dados = chamar_api_xfinance(url_transactions)

# Definindo Dataframes Pandas para dados gerados com a função
df = pd.DataFrame(transactions_dados)

# Importando funções da bibliotéca sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME
from sqlalchemy.exc import IntegrityError

# Ajustando colunas de data para as tabelas do Mysql
date_columns = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']


for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

# Declarando variáveis para paramentros de string de conexão com dados no .env
db_user = os.getenv("DB_USER")
db_passeword = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name =  os.getenv("DB_NAME")

# Declarando string de conexão para criação de engine de conexão com o DB
engine = create_engine(f'mysql+pymysql://{db_user}:{db_passeword}@{db_host}/{db_name}')

# Instanciando MetaData para criação de tabela
metadata = MetaData()

# Declarando modelo de tabela
transactions_table = Table('transactions', metadata,
    Column('Modified Date', DATETIME),
    Column('Created Date', DATETIME),
    Column('Created By', String(255)),
    Column('estimated_date', DATETIME),
    Column('recipient_ref', String(255)),
    Column('status', String(255)),
    Column('amount', DECIMAL(10,2)),
    Column('year_ref', Integer),
    Column('payment_date', DATETIME),
    Column('OS_type-transaction', String(255)),
    Column('user_ref', String(255)),
    Column('cod_ref', String(255)),
    Column('month_ref', Integer),
    Column('OS_frequency-type', String(255)),
    Column('_id', String(255), primary_key=True)
)

# Criando Tabela
metadata.create_all(engine)

# Inserindo dados na tabela MySql
inseridos_com_sucesso = 0

#Itera linha a linha do DataFrame
for index, row in df.iterrows():
    try:
        # Cria uma DataFrame de uma única linha e salva linha a linha no DB
        row.to_frame().T.to_sql('transactions', con=engine, if_exists='append', index=False)

        # Incremetando o contador de registros   para cada linha adicionada
        inseridos_com_sucesso += 1
    except IntegrityError as e:

        if 'Duplicated entry' in str(e.orig):
            print(f'Erro ao inserir dados (entrada duplicada): {e}')

        else:
            print(f'Erro ao inserir dados: {e}')

print(f'Total de registros inseridos com sucesso: {inseridos_com_sucesso}')