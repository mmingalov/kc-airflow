"""
My first simple dag.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import random

QUOTES = [
    "Nobody's perfect.", 
    "Be confident.",
    "Focus on your goals, not other people.",
    "Your mistakes don't define you.",
    "Never give up."
]

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm.alieva-2'
}

with DAG("alieva_dag_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def positive_quote_func():
        logging.info(random.choice(QUOTES))

    positive_quote = PythonOperator(
        task_id='positive_quote',
        python_callable=positive_quote_func
    )

    start >> positive_quote >> end