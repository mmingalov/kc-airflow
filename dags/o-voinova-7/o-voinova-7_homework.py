"""
Домашнее задание:

"""

from airflow import DAG
# from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging

# from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator



DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'o_voinova_7',
    'tags': ['o-voinova-7'],
}


with DAG("o-voinova-7_homework",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
         ) as dag:

    def is_not_sunday_check(ds: str) -> bool:
        exec_day = datetime.strptime(ds, '%Y-%m-%d').weekday()
        logging.info(f'execution_date: {ds}\nexec_day: {exec_day}')
        return exec_day != 6

    is_not_sunday_ = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_check,
        op_kwargs={'ds': '{{ds}}'},
    )

    def push_article_heading_(**kwargs):
        row_id = kwargs.get('data_interval_start').strftime('%w')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {row_id}")
        article_heading = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(value=article_heading, key='article_heading')

    push_article_heading = PythonOperator(
        task_id='push_article_heading',
        python_callable=push_article_heading_,
    )


    def pull_article_heading_(**kwargs):
        logging.info(kwargs['ti'].xcom_pull(task_ids='push_article_heading', key='article_heading'))


    pull_article_heading = PythonOperator(
        task_id='pull_article_heading',
        python_callable=pull_article_heading_,
    )


is_not_sunday_ >> push_article_heading >> pull_article_heading

dag.doc_md = __doc__

is_not_sunday_.doc_md = """Проверяет, что execution_date не воскресенье"""
push_article_heading.doc_md = """Забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds
                                 и отправляет в XCom название статьи"""
pull_article_heading.doc_md = """Получает из XCom название статьи и пишет в лог."""
