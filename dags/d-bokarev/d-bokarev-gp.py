"""
DAG для загрузки данных из GreenPlum по расписанию пн-сб
"""
import logging

from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600,
    'owner': 'd-bokarev'
}

with DAG("d-bokarev-gp",
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * 1-6',
    max_active_runs=1,
    tags=['d-bokarev']) as dag:

    def get_greeenplum_data(**kwargs):
        logging.info("Start logging")
        logging.info("DS: " + kwargs['ds'])
        gp_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        week_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday()+1
        logging.info("Weekday number is " + str(week_day))
        conn = gp_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = { week_day }')
        results = cursor.fetchall()
        return results

    get_greeenplum_data_operator=PythonOperator(task_id="get_greeenplum_data_operator",
                                                python_callable=get_greeenplum_data,
                                                provide_context=True)
    def print_result(** kwargs):
        logging.info(kwargs['templates_dict']['tbl_row_text'])

    print_result_operator=PythonOperator(task_id="print_result_operator",
                                         python_callable=print_result,
                                         templates_dict={'tbl_row_text': '{{ ti.xcom_pull(task_ids="get_greeenplum_data_operator", key="return_value") }}'},
                                         provide_context=True
                                         )

    get_greeenplum_data_operator >> print_result_operator
