import sys
import os

# Agregar la carpeta 'scripts' al PYTHONPATH
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../scripts'))

# Ahora podrás importar tus módulos desde 'scripts'
from etl.etl_data_sdk_flight_radar_airlines import run_etl  # Importación del proceso ETL

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Definir la función que ejecutará el proceso ETL
def execute_etl():
    run_etl()

# Configuración del DAG
with DAG(
    dag_id='dag_airlines',
    start_date=datetime(2024, 10, 20),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=execute_etl,
    )