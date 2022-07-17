from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from a_minkin_8_plugins.aminkin_ram_operator import RickAndMortyTopLocationsOperator
from datetime import datetime

default_args = {
    'owner': 'aminkin',
    'email': ['aminkin@cbgr.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022,1,1),
    'schedule_interval': None,
    'depends_on_past': False
}

@dag(
    dag_id='aminkin-ram-dag-v02',
    description='Test dag for rick and morty api',
    default_args=default_args,
    catchup=False
)
def aminkin_ram_dag():
    
    @task(
        task_id='create-ram-table'
    )
    def create_ram_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        print("Creating TABLE")
        cursor.execute(f"""create table if not exists aminkin_ram_location(
                    id				integer primary key,
                    name			text,
                    type			text,
                    dimension		text,
                    resident_cnt	integer,
                    gather_date		timestamp default CURRENT_TIMESTAMP
                );""")
        print("Clearing TABLE")
        cursor.execute("truncate table aminkin_ram_location;")
        conn.commit()

    ram_task = RickAndMortyTopLocationsOperator(task_id="gather-ram-locations", top_limit = 3)

    @task(
        task_id='save-top-locations'
    )
    def save_top_locations(**kwargs):
        ti = kwargs['ti']
        sql_values = ti.xcom_pull(task_ids='gather-ram-locations')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        print("Loading TABLE")
        cursor.execute(f"""INSERT INTO aminkin_ram_location(id, "name", "type", dimension, resident_cnt) VALUES\n{sql_values};""")
        conn.commit()

    create_ram_table() >> ram_task >> save_top_locations()

dag = aminkin_ram_dag()