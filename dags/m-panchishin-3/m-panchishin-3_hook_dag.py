"""
Даг 2
Сбор данных из Greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-panchishin-3',
    'poke_interval': 600
}


dag_params = {
    'dag_id': "m-panchishin-3_hook_dag",
    'default_args': DEFAULT_ARGS,    
    'schedule_interval': '0 15 * * 1-6',
    'max_active_runs': 1,
    'tags': ['m-panchishin-3'] 
}


with DAG(**dag_params) as dag:

    def xcom_ds(**kwargs):
        from datetime import datetime
        ds = datetime.strptime(kwargs['templates_dict']['a-gajdabura'], '%Y-%m-%d').weekday()+1
        return ds


    def gp_select(**kwargs):
        ti = kwargs['ti']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("select_gp")
        weekday = ti.xcom_pull(task_ids='get_execution_date')
        sql=f'SELECT heading FROM articles WHERE id = {weekday}'
        cursor.execute(sql)
        query_res = cursor.fetchone()[0]
        ti.xcom_push(value=query_res, key='article')


    start = DummyOperator (
        task_id = 'start'
    )


    end = DummyOperator(
        task_id='end'
    )
    

    get_execution_date = PythonOperator(
        task_id='get_execution_date',
        python_callable=xcom_ds,
        templates_dict={'a-gajdabura': '{{ a-gajdabura }}'},
    )

    
    push_greenplum = PythonOperator(
        task_id='push_greenplum',
        python_callable=gp_select,
        provide_context=True,
        )


    start >> get_execution_date >> push_greenplum >> end


    dag.doc_md = __doc__
    get_execution_date.doc_md = """Получение номера дня недели и передача его в XCOM"""
    push_greenplum.doc_md = """Получение результата работы предыдущего таска из XCOM и отправка его дальше в XCOM"""



