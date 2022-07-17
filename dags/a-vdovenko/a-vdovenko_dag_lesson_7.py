from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from a_vdovenko_plugins.RickMortyOperator import RickMortyParse, data_insertion


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-vdovenko',
    'poke_interval': 600
}

with DAG("a-vdovenko_rick_morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-vdovenko_lesson_7']
         ) as dag:

    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "a_vdovenko_lesson_7"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "a_vdovenko_lesson_7";'''
        ],
        autocommit=True
    )

    rick_morty_parse = RickMortyParse(
        task_id='rick_morty_parse')

    data_insertion_ = PythonOperator(
        task_id='data_insertion_',
        python_callable=data_insertion,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> rick_morty_parse >> data_insertion_ >> end
