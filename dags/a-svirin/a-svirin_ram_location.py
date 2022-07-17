"""
Даг находит 3 локации с наибольшим числом жителей во вселенной Рика и Морти.
"""
import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from a_svirin_plugins.a_svirin_operator import SvirinRaMOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-svirin'
}

with DAG("a-svirin-ram",
          schedule_interval='0 9 * * *',
          max_active_runs=1,
          default_args=DEFAULT_ARGS,
          tags=['a-svirin']) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id = 'conn_greenplum_write',
        sql=""" create table if not exists public.a_svirin_ram_location(
                id int,
                name text,
                type text,
                dimension text,
                resident_cnt int)
            DISTRIBUTED BY (id);""")

    clean_table = PostgresOperator(
        task_id='clean_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""TRUNCATE TABLE public.a_svirin_ram_location""")

    find_top_locations = SvirinRaMOperator(task_id='find_top_locations')

    def update(**kwargs):
        task_instance = kwargs['ti']

        top_locations = task_instance.xcom_pull(task_ids='find_top_locations')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        for index, row in top_locations.iterrows():
            sql_statement = """ INSERT INTO public.a_svirin_ram_location
                                VALUES ('{}', '{}', '{}', '{}', '{}');
                """.format(row['id'], row['name'], row['type'], row['dimension'], row['resident_cnt'])

            pg_hook.run(sql_statement, False)

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=update)


create_table >> clean_table >> find_top_locations >> load_data