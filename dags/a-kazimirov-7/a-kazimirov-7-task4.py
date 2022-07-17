from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

DEFAULT_ARGS = {
        'start_date': datetime(2022, 3, 1),
        'end_date': datetime(2022, 3, 14),
        'owner': 'a-kazimirov-7',
        'poke_interval': 600
    }

with DAG("a-kazimirov-7_dag_task4",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['kazimirov']
    ) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    def get_article_by_id(article_id):
        logging.info(f'SELECT heading FROM articles WHERE id = {article_id};')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        logging.info(cursor.fetchone())
        return cursor.fetchone()

    # get_article
    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_id,
        #op_args=["{{ execution_date.strftime('%w') }}", ],
        #Прибавим 2 к номеру дня, потому что по условию понедельнику
        #соответствует 1, а DAG от 1 марта по UTC запускается 28 февраля в 21:00.
        op_args=["{{ dag_run.logical_date.weekday() + 2 }}", ],
        do_xcom_push=True,
        dag=dag
    )

    start >> get_article >> end
