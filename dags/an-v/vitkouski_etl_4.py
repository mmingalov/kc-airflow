"""
Load currency rates and save to GP
"""
from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.utils.dates import days_ago
from airflow.utils.weekday import WeekDay


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'an-v',
    'poke_interval': 600
}

with DAG("vitkouski_etl_4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['an-v']
         ) as dag:
    day_sensor = DayOfWeekSensor(
        task_id='day_sensor',
        week_day=[WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY,
                  WeekDay.FRIDAY, WeekDay.SATURDAY],
        use_task_execution_day=True
    )


    def extract_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        article_id = date.today().weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        return cursor.fetchone()[0]


    extract_gp = PythonOperator(
        task_id='extract_gp',
        python_callable=extract_gp_func
    )

    day_sensor >> extract_gp
