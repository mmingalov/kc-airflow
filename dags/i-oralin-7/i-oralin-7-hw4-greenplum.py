"""
ДЗ Урок 4 (i-oralin-7)
"""
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# => pip install apache-airflow-providers-postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.timezone import datetime

DEFAULT_ARGS = {
    # 01.03.2022 to 14.03.2022
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-oralin-7',
    'poke_interval': 600
}

with DAG("i-oralin-7-hw4-greenplum",
         # CRON: run all days except sunday at 10:00
         # Can be also done with branching operator
         schedule_interval='0 10 * * 1-6',
         default_args=DEFAULT_ARGS,
         # max_active_runs=1,
         tags=['i-oralin-7'],
         # Don't run for all previous days
         # catchup=False
         ) as dag:
    dummy = DummyOperator(task_id='dummy')


    def get_article_heading_by_id(id, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("oralin-cursor")  # и именованный (необязательно) курсор
        cursor.execute(f'select heading from articles where id = {id}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        heading_result = query_res[0]
        # Log result
        logging.info(f"Got article (id = {id}) header:\n{heading_result}")
        # Also send result to XCom
        ti = kwargs['ti']
        ti.xcom_push('article_header', heading_result)


    get_article_heading = PythonOperator(
        task_id='get_article_heading',
        python_callable=get_article_heading_by_id,
        # op_args is in template_fields => can get values from jinja
        op_args=["{{ execution_date.strftime('%w') }}"]  # week day (0-6 => Sun is 0, Mon is 1 ... Sat is 6)
    )

    # This is one of the ways to run Postgres command but it can't log out or push result to Xcom
    # get_articles_postgres_op = PostgresOperator(
    #     task_id="get_articles",
    #     postgres_conn_id="conn_greenplum",
    #     # sql filed in template_fields so it is templated
    #     sql="select heading from articles where id = {{ execution_date.strftime('%w') }} + 1"
    # )

    dummy >> get_article_heading

    # Documentation
    dag.doc_md = __doc__

    dummy.doc_md = """Some dummy operator - it is not needed actually"""
    get_article_heading.doc_md = """Gets heading of article with id == (number of current day of the week)"""
