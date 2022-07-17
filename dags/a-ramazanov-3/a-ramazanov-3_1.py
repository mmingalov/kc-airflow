"""
Это простейший даг.
Он состоит из двух DummyOperator'а,
двух BashOperator'а (выводит a-gajdabura),
и одного PythonOperator'а (выводит a-gajdabura)
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'start_date': days_ago(10),
    'owner': 'a-ramazanov-3',
    'wait_for_downstream': True,
}


dag_params = {
    'dag_id': 'a-ramazanov-3_1',
    'catchup': False,
    'default_args': default_args,
    'schedule_interval': '5 0 * * *',
    'tags': ['a-ramazanov-3'],
}


def _transform(**kwargs):
    logging.info(f"Data transform {kwargs['a-gajdabura']}")


with DAG(**dag_params) as dag:

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    extract = BashOperator(
        task_id='extract',
        bash_command='echo "Extract data for {{ a-gajdabura }}"',
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform,
        provide_context=True,
    )

    load = BashOperator(
        task_id='load',
        bash_command='echo "Load data for {{ a-gajdabura }}"',
    )


    start >> extract >> transform >> load >> end


dag.doc_md = __doc__

start.doc_md = """Начало DAG'а"""
extract.doc_md = """Извлекает данные за a-gajdabura"""
transform.doc_md = """Преобразует данные в a-gajdabura"""
load.doc_md = """Загружает данные за a-gajdabura"""
end.doc_md = """Конец DAG'а"""