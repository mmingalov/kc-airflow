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
from a_ramazanov_3_plugins.operators.rickandmorty_top3_locations_operator import RAM_loc_operator


default_args = {
    'start_date': days_ago(1),
    'owner': 'a-ramazanov-3',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


dag_params = {
    'dag_id': 'a-ramazanov-3_3',
    'catchup': False,
    'default_args': default_args,
    'tags': ['a-ramazanov-3'],
    'schedule_interval': None,
}


with DAG(**dag_params) as dag:

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
            CREATE TABLE IF NOT EXISTS a_ramazanov_3_rickandmorty_top3_locations
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
            "TRUNCATE TABLE a_ramazanov_3_rickandmorty_top3_locations",
            "INSERT INTO a_ramazanov_3_rickandmorty_top3_locations VALUES {{ ti.xcom_pull(task_ids='xcom_push_rickandmorty_top3_location') }}",
        ], 
        autocommit=True,
    )


    start >> xcom_push_rickandmorty_top3_location >> create_table_if_not_exists >> load_top3_locations_gp >> end


start.doc_md = """Начало DAG'а"""
xcom_push_rickandmorty_top3_location.doc_md = """Помещает топ 3 локации в XCom"""
create_table_if_not_exists.doc_md = """Создаёт таблицу a_ramazanov_3_rickandmorty_top3_locations, если её нет"""
load_top3_locations_gp.doc_md = """Загружает данные в a_ramazanov_3_rickandmorty_top3_locations"""
end.doc_md = """Конец DAG'а"""
