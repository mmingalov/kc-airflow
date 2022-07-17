"""
Даг, который находит
три локации сериала "Рик и Морти"
с наибольшим количеством резидентов
и скалдывает их в таблицу в Greenplum.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from k_saprykina_4_plugins.top_locations_operator import TopLocations


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-saprykina-4',
    'poke_interval': 600
}

with DAG("k-saprykina-4_lesson5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['k-saprykina-4']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    get_top_locations_task = TopLocations(
        task_id='get_top_locations_task'
    )

    def write_to_greenplum(**context):
        top_df = context['ti'].xcom_pull(key='ksaprykina_ram')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        engine = pg_hook.get_sqlalchemy_engine()
        top_df.to_sql(name='k_saprykina_4_ram_location', con=engine, if_exists='replace')
        engine.dispose()

    greenplum_task = PythonOperator(
        task_id='greenplum_task',
        python_callable=write_to_greenplum
    )

    dummy >> get_top_locations_task >> greenplum_task


