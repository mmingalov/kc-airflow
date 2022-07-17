from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'an-shishkov',
    'poke_interval': 600
}

with DAG(
    dag_id='avs_get_data_from_greenplum',
    schedule_interval='1 10 * * 1,2,3,4,5,6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['an-shishkov']
) as dag:
    
    dummy_task = DummyOperator(task_id='dummy_task')
    
    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo "hello, airflow! it is bash"',
        dag=dag
    )
    
    def hello_airflow_func():
        msg = 'hello, airflow! it is python'
        logging.info(msg)
    
    hello_airflow_task = PythonOperator(
        task_id='python_task_1',
        python_callable=hello_airflow_func,
        dag=dag
    )
    
    def date_func():
        msg = datetime.today().strftime('%Y-%m-%d')
        logging.info(msg)
    
    date_task = PythonOperator(
        task_id='python_task_2',
        python_callable=date_func,
        dag=dag
    )
    
    def get_data_from_greenplum(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {dnum}'.format(dnum=exec_day))
        
        one_string = cursor.fetchone()[0]
        logging.info(one_string)
    
    gp_task = PythonOperator(
        task_id='python_task_3_greenplum',
        python_callable=get_data_from_greenplum,
        templates_dict={'execution_dt': '{{ ds }}'},
        dag=dag
    )
    
    dummy_task >> [echo_task, hello_airflow_task, date_task] >> gp_task
