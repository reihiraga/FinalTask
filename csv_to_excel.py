import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'csv_to_excel',
    default_args=default_args,
    description='A DAG to convert CSV to Excel',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def convert_csv_to_excel():
    # Membaca file CSV dari Google Cloud Storage
    csv_file = 'gs://asia-southeast1-btpn-enviro-7850ffe0-bucket/dags/output.csv'
    df = pd.read_csv(csv_file)
    
    # Mengubah DataFrame menjadi file Excel
    excel_file = 'gs://asia-southeast1-btpn-enviro-7850ffe0-bucket/dags/output.xlsx'
    df.to_excel(excel_file, index=False)

convert_csv_to_excel_task = PythonOperator(
    task_id='convert_csv_to_excel',
    python_callable=convert_csv_to_excel,
    dag=dag,
)