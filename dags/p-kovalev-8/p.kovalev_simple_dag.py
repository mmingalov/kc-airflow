"""

    A simple dag who does simple things:)

"""

import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'depends_on_past': False,
    'owner': 'p.kovalev@ancor.ru',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1)
}


@dag(
    dag_id='p.kovalev_simple_dag',
    catchup=True,
    start_date=pendulum.datetime(2022, 4, 23),
    end_date=pendulum.datetime(2022, 10, 1),
    schedule_interval='0 0 * * 1-6',
    tags=['p.kovalev'],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__
)
def dag_func():
    empty_task = DummyOperator(
        task_id='empty'
    )

    os_ver_task = BashOperator(
        task_id='os_ver',
        bash_command='cat /etc/os-release'
    )

    def print_log_date(**kwargs):
        print(f"ds = {kwargs['ds']}")

    log_date_task = PythonOperator(
        task_id='print_logical_date',
        python_callable=print_log_date
    )

    @task(task_id='get_article_heading', do_xcom_push=True)
    def get_article_heading(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT heading FROM articles WHERE id = {kwargs['logical_date'].isoweekday()}")
        res_heading = cursor.fetchone()[0]

        return res_heading

    get_article_task = get_article_heading()

    empty_task >> [os_ver_task, log_date_task]


dag = dag_func()
