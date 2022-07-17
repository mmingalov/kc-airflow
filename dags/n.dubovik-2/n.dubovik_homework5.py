"""
Даг создает таблицу в GreenPlum с названием 'n.dubovik-2_ram_location', если она не существует.
Перед записью данных стирает все содержимое таблицы.
Записывает в эту таблицу информацию о 3 самых многочисленных локациях, исходя из данных расположенных на https://rickandmortyapi.com.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dubovik_plugins.dubovik_operator import DubovikOperator

DEFAULT_ARGS = {
    'owner': 'n.dubovik-2',
    'email': ['nikolaidubovik@gmail.com'],
    'start_date': datetime(2021, 11, 1),
    'poke_interval': 300,
    'retries': 3
}

GP_TABLE_NAME = 'n_dubovik_2_ram_location'
GP_CONN = 'conn_greenplum_write'

with DAG(
        dag_id='n.dubovik_homework5',
        schedule_interval='0 0 * * 1-7',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['n.dubovik-2']
) as dag:

    start_time = BashOperator(
        task_id='log_start_time',
        bash_command='echo {{ a-gajdabura }}'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=GP_CONN,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {GP_TABLE_NAME} (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
        """
    )

    clean_table = PostgresOperator(
        task_id='clean_table',
        postgres_conn_id=GP_CONN,
        sql=f"""
            TRUNCATE {GP_TABLE_NAME};
        """,
        autocommit=True
    )

    load_locations = DubovikOperator(
        task_id='load_locations',
        pages_count=3,
        gp_table_name=GP_TABLE_NAME,
        gp_conn_id=GP_CONN
    )

    finish_time = BashOperator(
        task_id='log_finish_time',
        bash_command='echo {{ a-gajdabura }}'
    )

start_time >> create_table >> clean_table >> load_locations >> finish_time

dag.doc_md = __doc__
start_time.doc_md = """Пишет в логи время запуска DAG'а"""
create_table.doc_md = """Создает таблицу с названием 'n.dubovik-2_ram_location', если она не существует"""
clean_table.doc_md = """Удаляет содержимое таблицы"""
load_locations.doc_md = """Получает данные о 3 самых многочисленных локациях"""
finish_time.doc_md = """Пишет в логи время окончания DAG'а"""
