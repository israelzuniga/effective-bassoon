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

def drop_na():
    print('drop_na')
    df = pd.read_csv('c2k_raw.csv', index_col='nr')
    df.dropna(inplace=True)
    df.to_csv('c2k_dropn.csv')
	
def fill():
    print('3')
    df = pd.read_csv('c2k_dropn.csv', index_col='nr')
    df.replace(to_replace='?', value=0, inplace=True)
    df.to_csv('c2k_fill.csv')

def replace():
    print('replace')
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
    drop_na = PythonOperator(task_id='drop_na',
                                 python_callable=drop_na)
    replace = PythonOperator(task_id='replace',
                                 python_callable=replace)
    cast = PythonOperator(task_id='cast',
                                 python_callable=cast)

# Dependencies

drop_na.set_upstream(download)
replace.set_upstream(drop_na)
cast.set_upstream(replace)
