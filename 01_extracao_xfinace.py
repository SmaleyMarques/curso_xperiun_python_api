# %%

# Links de apoio:

# Postman - https://web.postman.co/ (Para teste de APIs)
# Json Viewer - https://jsonviewer.stack.hu/ (Para vizualização de json)
# HTTP Status Dogs - https://httpstatusdogs.com/ (Status Requests Explicado)

# %%

# SCRIPT PARA EXTRAÇÃO DE DADOS DA API DA XFINANCE

import requests
import pandas as pd

# Declarando Token para Autenticação
token = "c612a092017fc39036d10905bf6ce586"
headers = {"Authorization": f"Bearer {token}"}

url_category = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/category/"
url_recipient = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/recipient/"

# Definindo Função para conexão e extração de dados na API
def chamar_api_xfinance(url):
    # Fazendo requisição
    response = requests.get(url, headers=headers)

    return response # Retornando resposta

# Chamando função
response_category = chamar_api_xfinance(url_category)
response_recipient = chamar_api_xfinance(url_recipient)

# Acessando resultados a partir da resposta da requisição
category_json = response_category.json()['response']['results']
recipient_json = response_recipient.json()['response']['results']

# Definindo Dataframes Pandas para as resposta com apenas as colunas selecionadas
df_category = pd.DataFrame(category_json, columns=['_id', 'title'])
df_recipient = pd.DataFrame(recipient_json, columns=['_id', 'title', 'category_ref'])

# Salvando em excel e parquet com Pandas sem index

df_category.to_excel('data/category.xlsx', index=False)
df_recipient.to_excel('data/recipient.xlsx', index=False)

df_category.to_parquet('data/category.parquet', index=False)
df_recipient.to_parquet('data/recipient.parquet', index=False)