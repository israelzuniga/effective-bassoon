import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

bashCommand = "curl https://archive.ics.uci.edu/ml/machine-learning-databases/00382/c2k_data_comma.csv"
dataCleaned = null
dataConverted = null
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
dag = DAG('firstDAG', default_args={
    'owner': 'Jaime'
    'start_date': datetime(2018, 9, 1)
})

t1 = BashOperator(
    task_id='download',
    bash_command=bashCommand,
    dag=dag)

t2 = bashCommand(
    task_id='clean data',
    python_callable=cleanData,
    dag=dag)

t3 = bashCommand(
    task_id='convert float64',
    python_callable=convertFloat64,
    dag=dag)

t1 >> t2 >> t3