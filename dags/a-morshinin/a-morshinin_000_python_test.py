from airflow import DAG

import pandas as pd
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


test_dag = DAG(
    "a-morshinin_000_python_dag",
    start_date=days_ago(0, 0, 0, 0),
    tags = ['a-morshinin']
)


def download_titanic_dataset():
    df = pd.read_csv("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
    df.to_csv("df.csv")


download_dataframe = PythonOperator(
    task_id='download_titanic_dataset',
    python_callable=download_titanic_dataset,
    dag=test_dag
)


def transform_titanic_dataset():
    df = pd.read_csv("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
    gr = df.groupby("Pclass").agg({"Survived": "mean"})
    print ("В первом классе выжило: ", gr.iloc[0]['Survived'], " пассажиров")


transform_dataframe = PythonOperator(
    task_id='transform_dataframe',
    python_callable=transform_titanic_dataset,
    dag=test_dag
)

"""
Важно, чтобы эти 2 оператора запускались из одной директории. Поскольку 1 кладет файл, а другой его считывает.
Можно воспользоваться os.chdir("")
"""

transform_dataframe