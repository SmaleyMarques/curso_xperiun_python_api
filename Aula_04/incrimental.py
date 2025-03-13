# %%
# SCRIPT PARA EXTRAÇÃO DE DADOS DA API DA XFINANCE E INPUT NO MYSQL

import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME, inspect, text
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta
import json

# Instanciando arquivo .env para acessar TOKEN
load_dotenv()

# Declarando Token para Autenticação
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

url_transactions = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"

# Definindo Função para conexão e extração de dados na API com paginação
def chamar_api_xfinance(url, constraints=None):

    dados = [] # criando tabela para receber dados de resposta
    cursor = 0 # iniciando cursor em 0
    params = {'cursor': cursor} 

    if constraints:
        params['constraints'] = json.dumps(constraints)
        
    # Loop para paginação
    while True:

        # Fazendo requisição
        response = requests.get(url, headers=headers, params=params)
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
        params['cursor'] = cursor
        # Pausa de 1 segundo no código
        time.sleep(1)
    
    return dados

def chamar_api_inicial_completa():
    transactions_dados = chamar_api_xfinance(url_transactions)
    df = pd.DataFrame(transactions_dados)
    return df

def atualizar_dados_ultimos_dois_dias():
    # Calculando última data a dois dias atrás a partir de hoje
    dois_dias_atras = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

    # Definindo Constraints
    constraints =[{"key": "estimated_date", "constraint_type": "greater than", "value": dois_dias_atras}]

    # Chama a função para obter dados filtrados
    transactions_dados = chamar_api_xfinance(url_transactions, constraints)

    # Definindo Dataframes Pandas para dados gerados com a função
    df = pd.DataFrame(transactions_dados)
    return df


# Ajustando colunas de data para as tabelas do Mysql
date_columns = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']

# for col in date_columns:
#     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

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

# Instância um inspencionamento para o banco de dados
inspector = inspect(engine)

# Verifica se tabela existe
if 'transactions' in inspector.get_table_names():

    # Criando contexto de conexão temporária
    with engine.connect() as connection:

        # Roda um SELECT COUNT para verificação de quantidade de registros
        result = connection.execute(text("SELECT COUNT(*) FROM transactions"))
        count = result.fetchone()[0]

        # Verifica se existe registros na tabela
        if count > 0:
            df_incremental = atualizar_dados_ultimos_dois_dias()
            if df_incremental.empty:
                print("Não há dados para atualizar")
            else:
                for col in df_incremental:
                    if col in date_columns:
                        df_incremental[col] = pd.to_datetime(df_incremental[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                inseridos_com_sucesso = 0

                for index, row in df_incremental.iterrows():
                    try:
                        row.to_frame().T.to_sql('transactions', con=engine, if_exists='append', index=False)
                        inseridos_com_sucesso += 1
                    except IntegrityError:
                        continue
                print(f'Total de registros inseridos com sucesso: {inseridos_com_sucesso}')
        else:
            print("Tabela existente mas sem dados, realizando carga inicial completa")
            df_inicial = chamar_api_inicial_completa()

else:
    print("Tabela não existente, criando tabela e realizando carga inicial completa")
    metadata.create_all(engine)
    df_inicial = chamar_api_inicial_completa()

# Verifica se df_inicial foi criando e insere dados no DB
if 'df_inicial' in locals():
    for col in df_inicial:
        if col in date_columns:
            df_inicial[col] = pd.to_datetime(df_inicial[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
    inseridos_com_sucesso = 0

    for index, row in df_inicial.iterrows():
        try:
            row.to_frame().T.to_sql('transactions', con=engine, if_exists='append', index=False)
            inseridos_com_sucesso += 1
        except IntegrityError:
            continue
    print(f'Total de registros inseridos com sucesso: {inseridos_com_sucesso}')