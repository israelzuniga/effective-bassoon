import pandas as pd
import datetime as dt

from time import sleep
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00382/c2k_data_comma.csv"

def cargararchivo():
	print ('Carga Archivo')
	datos = pd.read_csv(url)
	datos.to_csv('archivo_paso1.csv')

def eliminarna():
	print ('Elimina los registros NA')
	datos = pd.read_csv('archivo_paso1.csv', index_col='nr')
	datos.dropna()
	datos.to_csv('archivo_paso2.csv')

def remplazar():
	print ('Reemplaza el caracter raro ?')
	datos = pd.read_csv('archivo_paso2.csv', index_col='nr')
	datos.replace('?',0)
	datos.to_csv('archivo_paso3.csv')

def cambiartipodato():
	print ('Cambia al tipo de datos float64')
	datos = pd.read_csv('archivo_paso3.csv', index_col='nr')
	datos.astype('float64')
	datos.to_csv('archivo_final.csv')

default_args = {
    'owner': 'Rocio Alvarez',
    'start_date': dt.datetime(2018, 9, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=4),
}

with DAG('Ejercicio Rocio Alvarez',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:

    cargararchivo = PythonOperator(task_id='download',
                                 python_callable=cargararchivo)
    eliminarna = PythonOperator(task_id='eliminarna',
                                 python_callable=eliminarna)
    remplazar = PythonOperator(task_id='remplazar',
                                 python_callable=remplazar)
    cambiartipodato = PythonOperator(task_id='cambiartipodato',
                                 python_callable=cambiartipodato)

# Dependencies

eliminarna.set_upstream(cargararchivo)
remplazar.set_upstream(eliminarna)
cambiartipodato.set_upstream(remplazar)