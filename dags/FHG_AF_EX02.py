import datetime as dt
from time import sleep
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd


url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00382/c2k_data_comma.csv"

def download():
    print('download')
    df = pd.read_csv(url)
    df.to_csv('c2k_raw.csv')

def dropn():
    print('dropn')
    df = pd.read_csv('c2k_raw.csv', index_col='nr')
    df.dropna(inplace=True)
    df.to_csv('c2k_dropn.csv')

def fill():
    print('fill')
    df = pd.read_csv('c2k_dropn.csv', index_col='nr')
    df.replace(to_replace='?', value=0, inplace=True)
    df.to_csv('c2k_fill.csv')

def cast():
    print('cast')
    df = pd.read_csv('c2k_fill.csv', index_col='nr')
    df = df.astype('float64')
    df.to_csv('dags/c2k_final.csv')


default_args = {
    'owner': 'Felix Hernandez',
    'start_date': dt.datetime(2018, 9, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('flow_pandas',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:

    download = PythonOperator(task_id='download',
                                 python_callable=download)
    dropn = PythonOperator(task_id='dropn',
                                 python_callable=dropn)
    fill = PythonOperator(task_id='fill',
                                 python_callable=fill)
    cast = PythonOperator(task_id='cast',
                                 python_callable=cast)

# Dependencies

dropn.set_upstream(download)
fill.set_upstream(dropn)
cast.set_upstream(fill)
