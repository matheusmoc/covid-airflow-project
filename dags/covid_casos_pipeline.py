import os
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

dataset = Variable.get("DATASET_BRASIL_IO")
table = Variable.get("TABLE_BRASIL_IO")
token = Variable.get("TOKEN_BRASIL_IO")

class DataExtractor:
    def __init__(self, dataset: str, table: str, token: str):
        self.dataset = dataset
        self.table = table
        self.token = token
        self.base_url = "https://api.brasil.io/v1/dataset"

    def get_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/{self.table}/data/"

    def fetch_data(self) -> dict:
        url = self.get_url()
        headers = {"Authorization": f"Token {self.token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def save_to_csv(self, data: dict, file_path: str):
        """
        Salva os resultados da API em um arquivo CSV.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        pd.DataFrame(data['results']).to_csv(file_path, index=False)
        print(f"Dados salvos em {file_path}")

extractor = DataExtractor(dataset, table, token)

def extract_data():
    """
    Extrai os dados usando o objeto extractor e salva no arquivo.
    """
    data = extractor.fetch_data()
    base_dir = os.path.abspath(os.path.dirname(__file__)) 
    raw_file_path = os.path.join(base_dir, 'data', 'raw', 'covid_data.csv')
    extractor.save_to_csv(data, raw_file_path)

def transform_data():
    """
    Transforma os dados e salva em outro arquivo.
    """
    base_dir = os.path.abspath(os.path.dirname(__file__))  
    raw_file_path = os.path.join(base_dir, 'data', 'raw', 'covid_data.csv')
    df = pd.read_csv(raw_file_path)
    
    processed_file_path = os.path.join(base_dir, 'data', 'processed', 'covid_data_transformed.csv')
    
    df['media_movel'] = df['confirmed'].rolling(window=7).mean()
    df['variacao_semanal'] = df['confirmed'].pct_change(periods=7)
    

    os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
    df.to_csv(processed_file_path, index=False)
    print(f"Dados transformados e salvos em {processed_file_path}")


def load_data():
    """
    Carrega os dados transformados no PostgreSQL usando pandas.
    """
    base_dir = os.path.abspath(os.path.dirname(__file__))  
    processed_file_path = os.path.join(base_dir, 'data', 'processed', 'covid_data_transformed.csv')
    
    df = pd.read_csv(processed_file_path)
    
    db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow" 
    engine = create_engine(db_url)
    
    df.to_sql('covid_data', engine, if_exists='replace', index=False)
    print("Dados carregados no PostgreSQL com sucesso.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'covid_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados COVID-19 do Brasil.IO',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql=""" 
            CREATE TABLE IF NOT EXISTS covid_data (
                state VARCHAR(255),
                city VARCHAR(255),
                confirmed INTEGER,
                deaths INTEGER,
                media_movel FLOAT,
                variacao_semanal FLOAT
            );
        """,
    )

    base_dir = os.path.abspath(os.path.dirname(__file__)) 
    processed_file_path = os.path.join(base_dir, 'data', 'processed', 'covid_data_transformed.csv')

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )


    extract_task >> transform_task >> create_table_task >> load_task
