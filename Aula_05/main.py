# Importa as bibliotecas necessárias
import requests  # Biblioteca para fazer requisições HTTP
import pandas as pd  # Biblioteca para manipulação de dados
import json  # Biblioteca para manipulação de dados em formato JSON
from google.cloud import bigquery  # Biblioteca do Google Cloud para manipulação do BigQuery
import os  # Biblioteca para manipulação de variáveis de ambiente
from datetime import datetime, timedelta  # Bibliotecas para manipulação de datas

# Função para chamar a API MyFinance
def chamar_api_myfinance(url, headers, constraints=None):
    print("Chamando API MyFinance...")
    lista_dados_todas_paginas = []  # Lista para armazenar os dados de todas as páginas
    cursor = 0  # Inicializa o cursor para paginação
    params = {"cursor": cursor}  # Define os parâmetros da requisição
    if constraints:
        params["constraints"] = json.dumps(constraints)  # Adiciona as restrições aos parâmetros

    while True:
        # Faz a requisição à API
        response = requests.get(url, headers=headers, params=params)
        # Converte a resposta para JSON
        response_ajustado_json = response.json()
        # Obtém a parte "response" do JSON
        dados_response = response_ajustado_json.get("response", None)
        
        if dados_response is not None:
            # Obtém os resultados da resposta
            results = dados_response.get('results', [])
            # Obtém a quantidade restante de dados a serem buscados
            remaining = dados_response.get('remaining', 0)
            # Adiciona os resultados à lista
            lista_dados_todas_paginas.extend(results)
            
            if remaining <= 0:
                break  # Encerra o loop se não há mais dados a buscar
            else:
                cursor += len(results)  # Atualiza o cursor para a próxima página
                params["cursor"] = cursor
        else:
            break  # Encerra o loop se não há dados na resposta

    print("API chamada com sucesso, dados obtidos.")
    # Retorna os dados como um DataFrame do Pandas
    return pd.DataFrame(lista_dados_todas_paginas)

# Função para atualizar dados dos últimos dois dias
def atualizar_dados_ultimos_dois_dias(url, headers):
    dois_dias_atras = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    constraints = [{"key": "estimated_date", "constraint_type": "greater than", "value": dois_dias_atras}]
    df_incremental = chamar_api_myfinance(url, headers, constraints)
    return df_incremental

# Função para carregar todos os dados
def carregar_todos_os_dados(url, headers):
    df_inicial = chamar_api_myfinance(url, headers)
    return df_inicial

# Função principal que coordena a execução do script
def main(request=None):
    print("Iniciando execução do script...")
    # Obtém IDs do projeto a partir das variáveis de ambiente
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = "xfinance"
    table_name = "transactions"
    
    # Cria um cliente do BigQuery utilizando as credenciais padrão do ambiente
    client = bigquery.Client(project=project_id)
    print("Conectado ao serviço BigQuery.")
    
    dataset_full_id = f"{client.project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_full_id)
    dataset.location = "US"

    try:
        # Verifica se o dataset existe
        client.get_dataset(dataset)
        print(f"Dataset {dataset_id} já existe.")
    except:
        # Cria o dataset se não existir
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Dataset {dataset_id} criado com sucesso.")
    
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions/"
    token = os.getenv("API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    try:
        # Verifica se a tabela existe
        table = client.get_table(table_id)
        table_exists = True
        print(f"Tabela {table_id} encontrada.")
    except:
        table_exists = False
        print(f"Tabela {table_id} não encontrada, será criada.")

    if table_exists:
        # Verifique se a tabela tem dados
        query = f"SELECT COUNT(*) as total_rows FROM `{table_id}`"
        query_job = client.query(query)
        results = query_job.result()
        total_rows = [row.total_rows for row in results][0]

        if total_rows > 0:
            # Atualização incremental
            query = f"SELECT MAX(`Modified Date`) as last_update FROM `{table_id}`"
            query_job = client.query(query)
            results = query_job.result()
            last_update = [row.last_update for row in results][0]

            if last_update:
                constraints = [{"key": "Modified Date", "constraint_type": "greater than", "value": last_update}]
                print(f"Última atualização encontrada: {last_update}. Aplicando constraints.")
                df_incremental = chamar_api_myfinance(url, headers, constraints)
            else:
                print("Nenhuma última atualização encontrada. Carregando todos os dados.")
                df_incremental = carregar_todos_os_dados(url, headers)

            if not df_incremental.empty:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job = client.load_table_from_dataframe(df_incremental, table_id, job_config=job_config)
                job.result()
                print(f"Total de registros inseridos com sucesso: {len(df_incremental)}")
                return f"Total de registros inseridos com sucesso: {len(df_incremental)}"
            else:
                print("Nenhum dado novo para inserir.")
                return "Nenhum dado novo para inserir."
        else:
            # Carga completa inicial
            df_inicial = carregar_todos_os_dados(url, headers)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(df_inicial, table_id, job_config=job_config)
            job.result()
            print(f"Total de registros inseridos com sucesso: {len(df_inicial)}")
            return f"Total de registros inseridos com sucesso: {len(df_inicial)}"
    else:
        # Criação da tabela e carga completa inicial
        df_inicial = carregar_todos_os_dados(url, headers)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_inicial, table_id, job_config=job_config)
        job.result()
        print(f"Total de registros inseridos com sucesso: {len(df_inicial)}")
        return f"Total de registros inseridos com sucesso: {len(df_inicial)}"

if __name__ == "__main__": # Se o script for executado diretamente
    main() # Chama a função principal
