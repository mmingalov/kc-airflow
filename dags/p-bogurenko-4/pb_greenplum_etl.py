


from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'p-bogurenko-4',
    'poke_interval': 600
}

with DAG("pb_greenplum_etl",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['pb_greenplum_etl']
         ) as dag:

    def get_article_on_weekday_func(**kwargs):
        logging.info('exec_date ' + kwargs['a-gajdabura'])
        weekday_exec_date = str(datetime.datetime.strptime(kwargs['a-gajdabura'], '%Y-%m-%d').weekday()+1)
        logging.info('for '+kwargs['a-gajdabura']+' weekday is '+weekday_exec_date)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("pb_data_collector")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday_exec_date))
        header = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(value=header, key=weekday_exec_date)

    article_head_to_xcom_on_weekday = PythonOperator(
        task_id='get_article_on_weekday',
        python_callable=get_article_on_weekday_func,
        dag=dag,
        provide_context=True
    )

    article_head_to_xcom_on_weekday

