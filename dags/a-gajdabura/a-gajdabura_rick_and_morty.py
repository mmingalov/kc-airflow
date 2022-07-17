"""
Забирает из https://rickandmortyapi.com/api/location
данные о локации, считает количество резидентов и
помещает в GreenPlum
"""


from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_gajdabura_plugins.operators.rickandmorty_top3_locations_operator import RickAndMortyLocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-gajdabura',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


with DAG('a-gajdabura_top3_location',
    max_active_runs=1,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['a-gajdabura'],
    schedule_interval=None,
    ) as dag:

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    xcom_push_rickandmorty_top3_location = RickAndMortyLocationOperator(
        task_id='xcom_push_rickandmorty_top3_location',
    )

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS a_gajdabura_ram_location
            (
                id integer PRIMARY KEY,
                name varchar(1024),
                type varchar(1024),
                dimension varchar(1024),
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True,
    )

    load_top3_locations_gp = PostgresOperator(
        task_id='load_top3_locations_gp',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE a_gajdabura_ram_location",
            "INSERT INTO a_gajdabura_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_rickandmorty_top3_location') }}",
        ],
        autocommit=True,
    )


    start >> xcom_push_rickandmorty_top3_location >> create_table_if_not_exists >> load_top3_locations_gp >> end


start.doc_md = """Начало DAG'а"""
xcom_push_rickandmorty_top3_location.doc_md = """Помещает топ 3 локации в XCom"""
create_table_if_not_exists.doc_md = """Создаёт таблицу a_gajdabura_ram_location, если её нет"""
load_top3_locations_gp.doc_md = """Загружает данные в a_gajdabura_ram_location"""
end.doc_md = """Конец DAG'а"""
