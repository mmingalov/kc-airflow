import logging
from datetime import datetime

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-kozlovski',
    'poke_interval': 600,
    'trigger_rule':  'all_success'
}

with DAG('d-kozlovski-test-dag',
         schedule_interval='0 10 * * MON,TUE,WED,THU,FRI,SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs = 1,
         tags=['d-kozlovski']
    ) as dag:

    #BashOperator с датой выполнения
    run_this = BashOperator(
        task_id='run_this',
        bash_command='echo "exec_dttm = {{ ds }}"',
        )

    def is_not_saturday_func(**kwargs):
        print(kwargs)
        exec_day = datetime.strptime(kwargs['execution_dt'], '%Y-%m-%d').weekday() + 1
        print(exec_day)
        kwargs['ti'].xcom_push(value = exec_day, key='weekday')
        return exec_day != 7

    not_saturday = ShortCircuitOperator(
        task_id='not_saturday',
        python_callable=is_not_saturday_func,
        op_kwargs= {'execution_dt': '{{ ds }}'}
        )

    def get_weekday_article_func(**kwargs):
        #get weekday from xcom
        weekday = kwargs['ti'].xcom_pull(task_ids='not_saturday', key='weekday')
        #query
        query = "SELECT heading FROM articles WHERE id = {weekday}".format(weekday = str(weekday))
        #get conn
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("cursor")  # и именованный (необязательно) курсор

        cursor.execute(query)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res)

    get_weekday_article = PythonOperator(
        task_id = 'get_weekday_article',
        python_callable = get_weekday_article_func
    )

    run_this >> not_saturday >> get_weekday_article
