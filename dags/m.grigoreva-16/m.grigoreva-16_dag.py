"""
DAG #1
"""

"""
Gets article heading with id that equals to day of week
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime



DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm.grigoreva-16',
    'poke_interval': 600
}

with DAG("m.grigoreva-16",
    schedule_interval='0 0 * * 1-6',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m.grigoreva-16_tag']
) as dag:

    def get_gp_data_func(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_cursor")
        cursor.execute('SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info(query_res)


    get_gp_data = PythonOperator(
        task_id='get_gp_data',
        python_callable=get_gp_data_func,
        op_args=['{{ logical_date.weekday() + 1}}'],
        dag=dag
    )

    get_gp_data