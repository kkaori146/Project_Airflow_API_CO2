from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import json
from pandas.io.json import json_normalize
import pandas as pd
import os

# Instalação do fastparqute

os.system('pip install fastparquet')


# Função para armazenamento do arquivo json

def armazenar_dados(ti) -> None:
  posts = ti.xcom_pull(task_ids=['extrator_api'])
  with open('dados_brutos/carbon_dioxide.json', 'w') as f:
    json.dump(posts[0], f)

# Função para normalização do arquivo json e armazenamento em formato csv

def normalizacao_json():
  dfjson = pd.read_json('dados_brutos/carbon_dioxide.json')
  dfcsv = pd.json_normalize(dfjson['co2'])

# Armazenamento do dado em formato csv

  dfcsv.to_csv('dados_brutos/carbon_dioxide.csv', index=False, sep=';', encoding='utf-8')

# Função para tratamento dos dados

def tratamento_dados():
  dfler = pd.read_csv('dados_brutos/carbon_dioxide.csv', sep = ';')

  # Tradução dos nomes das colunas

  dfler.rename(columns = {
    'year': 'ano',
    'month': 'mes',
    'day': 'dia',
    'cycle': 'ciclo_ppm',
    'trend': 'tendencia_ppm'}, inplace=True)

  # Padronizando para 2 casas decimais depois da vírgula

  dfler.loc[:, "ciclo_ppm"] = dfler["ciclo_ppm"].map('{:.2f}'.format)

  # Convertendo de string para float

  dfler['ciclo_ppm'] = dfler['ciclo_ppm'].astype(float)

  # Padronizando para 2 casas decimais depois da vírgula

  dfler.loc[:, "tendencia_ppm"] = dfler["tendencia_ppm"].map('{:.2f}'.format)

  # Convertendo de string para tipo float

  dfler['tendencia_ppm'] = dfler['tendencia_ppm'].astype(float)

  return dfler

# Função para exportação em formato csv e parquet

def exportacao_dados(**kwargs):
  ti = kwargs['ti']
  dfler = ti.xcom_pull(task_ids = 'tratamento_dados')

  # Conversão do dataset em arquivos csv e parquet

  dfler.to_csv('dados_tratados/carbon_dioxide_rate.csv', index=False)

  dfler.to_parquet('dados_tratados/carbon_dioxide_rate.parquet', index = False)

# Função para extração de informações quanto a quantidade total de Dióxido de Carbono anual em relação à tendência/previsão

def total_co2():
  dfanalise = pd.read_parquet('dados_tratados/carbon_dioxide_rate.parquet', engine = 'fastparquet')
  dfanalise = dfanalise.groupby(['ano'])['ciclo_ppm', 'tendencia_ppm'].sum().reset_index()

# Armazenamento em formato parquet dentro da ssubpasta pesquisa

  dfanalise.to_parquet('dados_tratados/pesquisa/carbon_dioxide_analysis.parquet', index = False)

# Definindo alguns argumentos básicos
default_args = {
    'owner':'kkaori146',
    'start_date': datetime(2022,11,22),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

# Instanciando a DAG:
with DAG(
    'api_carbon',
    max_active_runs=2,
    schedule_interval="@daily",
    #template_searchpath= '/opt/airflow/sql',
    catchup = True,
    default_args = default_args) as dag:
    

  verificador_api = HttpSensor(
      task_id = 'verificador_api',
      http_conn_id = 'api_carbon_dioxide',
      endpoint='/api/co2-api'
  )

  extrator_api = SimpleHttpOperator(
    task_id = 'extrator_api',
    http_conn_id= 'api_carbon_dioxide',
    endpoint= '/api/co2-api',
    method='GET',
    response_filter=lambda response: json.loads(response.text),
    log_response = True
  )

  armazenar_dados = PythonOperator(
    task_id = 'armazenar_dados',
    provide_context = True,
    python_callable = armazenar_dados
  )

  normalizacao_json = PythonOperator(
    task_id = 'normalizacao_json',
    python_callable = normalizacao_json
  )

  tratamento_dados = PythonOperator(
    task_id = 'tratamento_dados',
    python_callable = tratamento_dados
  )

  exportacao_dados = PythonOperator(
    task_id = 'exportacao_dados',
    python_callable = exportacao_dados 
  )

  total_co2 = PythonOperator(
    task_id = 'total_co2',
    python_callable = total_co2
  )

  [verificador_api, extrator_api] >> armazenar_dados >> normalizacao_json >> tratamento_dados >> exportacao_dados >> total_co2

