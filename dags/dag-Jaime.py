import pandas as pd
import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

bashCommand = "curl https://archive.ics.uci.edu/ml/machine-learning-databases/00382/c2k_data_comma.csv"
dataCleaned = None
dataConverted = None
def cleanData():
  t = pd.read_csv('c2k_data_comma.csv')
  dataFrame = pd.DataFrame(t)
  c = dataFrame.dropna()
  dataCleaned = c.replace('?', 0)

def convertFloat64():
  t = pd.read_csv('c2k_data_comma.csv')
  dataFrame = pd.DataFrame(t)
  dataConverted = dataFrame.astype('float64')

# create DAG instance
default_args = {
    'owner': 'Jaime',
    'start_date': dt.datetime(2018, 9, 1)
}
with DAG('firstDAG',
         default_args=default_args
        ) as dag:

  t1 = BashOperator(
    task_id='download',
    bash_command=bashCommand)

  t2 = PythonOperator(
    task_id='cleanedData',
    python_callable=cleanData)

  t3 = PythonOperator(
    task_id='convertToFloat64',
    python_callable=convertFloat64)

t1 >> t2 >> t3