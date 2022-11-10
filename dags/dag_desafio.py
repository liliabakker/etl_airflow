from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import numpy as np
import pandas as pd
import requests


#Função para extrair dados:
def extrair_dados():
    url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-etanol-anidro-hidratado-m3-2012-2022.csv'
    x = requests.get(url, verify=False)
    open('dados_brutos/producao-etanol-anidro-hidratado-m3-2012-2022.csv', 'wb').write(x.content)

#Função para ler dados e tratar:
def tratar_dados():
    df = pd.read_csv('dados_brutos/producao-etanol-anidro-hidratado-m3-2012-2022.csv', sep=';')
    df = df.rename(columns={'GRANDE REGIÃO': 'REGIÃO', 'PRODUTO':'ETANOL', 'PRODUÇÃO': 'PRODUÇÃO_m3'})
    df = df.loc[(df['ANO'] >= 2020) & (df['ANO'] < 2023)]
    df['PRODUÇÃO_m3'] = df['PRODUÇÃO_m3'].apply(lambda x: str(x).replace(",",".")).astype(float)
    df = df.groupby(['REGIÃO','ANO', 'ETANOL']).agg({'PRODUÇÃO_m3': 'sum'}).sort_values(by='ANO', ascending=True)
    return df
   
#Função para converter o df tratado em csv e parquet:
def converter_dados(**kwargs):
    ti=kwargs['ti']
    df = ti.xcom_pull(task_ids='tratar_dados')
    df.to_csv('dados_tratados/producao-etanol-anidro-hidratado-m3-2020-2022.csv', sep=';')
    df.to_parquet('dados_tratados/producao-etanol-anidro-hidratado-m3-2020-2022.pq', index=False)


# Definindo alguns argumentos básicos
default_args = {
    'owner':'Lilia',
    'start_date': datetime(2022,11,8),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

#Criando a DAG:
with DAG(
    'dag_desafio_2',
    max_active_runs=1,
    schedule_interval=None,
    default_args = default_args
) as dag:

  start = DummyOperator(
        task_id= 'start'
  )
  extrair = PythonOperator(
      task_id = 'extrair_dados',
      python_callable = extrair_dados
  )
  tratar= PythonOperator(
      task_id = 'tratar_dados',
      python_callable = tratar_dados
  )
  converter = PythonOperator(
      task_id = 'converter_dados',
      python_callable = converter_dados
  )
  end = DummyOperator(
    task_id='end')
  
start >> extrair >> tratar >> converter >> end