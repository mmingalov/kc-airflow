from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'owner': 'v-vasileva-3',
    'poke_interval': 600
}

with DAG("v-vasileva-3_dag2",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['v-vasileva-3']
        ) as dag:

    begin = DummyOperator(task_id='begin')

    end = DummyOperator(task_id='end')

    def get_wd_num():
        wd_num = datetime.datetime.today().weekday()+1
        return wd_num


    ex_wd_num = PythonOperator(
        task_id='ex_wd_num',
        python_callable=get_wd_num,
        dag=dag
    )


    def connect_greenplum(**kwargs):
        ti = kwargs['ti']
        wd_num = ti.xcom_pull(task_ids='ex_wd_num')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd_num};')
        query_res = cursor.fetchall()
        ti.xcom_push(value=query_res, key='wd_num')

    greenplum_connect = PythonOperator(
        task_id='connect_greenplum',
        python_callable=connect_greenplum,
        dag=dag
    )

    begin >> ex_wd_num >> greenplum_connect >> end