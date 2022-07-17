"""
Load articles from GreenPlum database every day except Sunday and put results into XCOM
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.weekday import WeekDay

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.weekday import DayOfWeekSensor

from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa


_DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-kulagin-4',
    'poke_interval': 600
}


def get_article_by_id(article_id):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("gp_conn")
    cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
    return cursor.fetchone()[0]


with DAG("d-kulagin-4_articles",
         schedule_interval='@daily',
         default_args=_DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-kulagin-4', 'load_articles']) as dag:

    mon_sat_sensor = DayOfWeekSensor(
        task_id='from_mon_to_sat',
        week_day={WeekDay.MONDAY,
                  WeekDay.TUESDAY,
                  WeekDay.WEDNESDAY,
                  WeekDay.THURSDAY,
                  WeekDay.FRIDAY,
                  WeekDay.SATURDAY},
        use_task_execution_day=True,
        dag=dag,
    )

    get_article_op = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}',],
        do_xcom_push=True,
    )

    mon_sat_sensor >> get_article_op
