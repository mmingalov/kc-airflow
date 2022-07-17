"""
Это простейший даг.
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-panchishin-3',
    'poke_interval': 600
}

dag_params = {
    'dag_id': "m-panchishin-3_simple_dag",
    'default_args': DEFAULT_ARGS,    
    'schedule_interval': '@daily',
    'max_active_runs': 1,
    'tags': ['m-panchishin-3'] 
}

with DAG(**dag_params) as dag:


    start_dag = DummyOperator (
        task_id = 'start_dag'
        )


    bash_jinja_params_output = BashOperator(task_id='bash_jinja_params_output',
                           bash_command='''
                                echo \">>> Start Writing <<<\";
                                echo \"DAG: {{ dag }}\"; 
                                echo \"Task: {{ task }}\"; 
                                echo \"Task Instance: {{ ti }}\"; 
                                echo \"Run ID: {{ run_id }}\"; 
                                echo \"Logical Date: {{ a-gajdabura }}\"; 
                                echo \">>> Stop Writing <<<\";
                            ''',
                           dag=dag)


    def first_func(**kwargs):
        task = kwargs['templates_dict']['task']
        ds = kwargs['templates_dict']['a-gajdabura']
        logging.info("First PythonOperator {0} DS: {1}".format(task, ds))


    python_task_1 = PythonOperator(task_id='python_task_1',
                                python_callable=first_func,
                                templates_dict={'task': '{{ task }}',
                                                'a-gajdabura': '{{ a-gajdabura }}'},
                                dag=dag)


    def second_func(**kwargs):
        task = kwargs['templates_dict']['task']
        ds = kwargs['templates_dict']['a-gajdabura']
        logging.info("Second PythonOperator {0} DS: {1}".format(task, ds))


    python_task_2 = PythonOperator(task_id='python_task_2',
                                 python_callable=second_func,
                                 templates_dict={'task': '{{ task }}',
                                                 'a-gajdabura': '{{ a-gajdabura }}'},
                                 dag=dag)

    start_dag >> bash_jinja_params_output >> [python_task_1, python_task_2]

    dag.doc_md = __doc__

    bash_jinja_params_output.doc_md = """Пишет в лог несколько параметров из темплейтов"""
    python_task_1.doc_md = """Пишет в лог 'First PythonOperator'"""
    python_task_2.doc_md = """Пишет в лог 'Second PythonOperator'"""
