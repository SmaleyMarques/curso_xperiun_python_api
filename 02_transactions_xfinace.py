# %%

# SCRIPT PARA EXTRAÇÃO DE DADOS COM PAGINAÇÃO DA API DA XFINANCE

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

# Salvando em excel e parquet com Pandas sem index
df.to_excel('data/transactions.xlsx', index= False)
df.to_parquet('data/transactions.parquet', index= False)
