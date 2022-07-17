"""
даг для задания уроков 3 и 4 - choosing best pub + get heading

useful links (notes for myself):
https://www.astronomer.io/events/recaps/trigger-dags-any-schedule/
https://forum.astronomer.io/t/airflow-start-date-concepts/393


"""

from airflow import DAG

from datetime import timedelta
import logging

from airflow.utils.dates import days_ago
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from random import randint
import pendulum

DEFAULT_ARGS = {
    # 'start_date': datetime(2022, 2, 28),
    # 'end_date': datetime(2022, 3, 15),
    'start_date': pendulum.datetime(2022, 2, 28, tz='UTC'),
#    'end_date': pendulum.datetime(2022, 3, 15, tz='UTC'),

    'owner': 'a-svechkov-8',
    'poke_interval': 600,
    # 'max_active_runs': 1,
    # 'depends_on_past': True,
    'catchup': True

}


def _choosing_best_pub(ti):
    ratings = ti.xcom_pull(task_ids=[
        'pub_1',
        'pub_2',
        'pub_3'
    ])
    if max(ratings) > 8:
        return 'go_to_pub'
    return 'go_home'


def _pub_ranking_generator(model):
    return randint(1, 10)

# def _get_heading_func():
def _get_heading_func(weekday):
    # how to get the dag execution date? - {{ ds }}
    # what arguments (if any) does the function need? =))
    #date_cur = {{ ds }}
    # weekday = date_cur.weekday()+1 # in gp id starts from 1
    weekday = int(weekday)+1 # in gp id starts from 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
    # query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(str(date_cur) + ' : ' + one_string)

with DAG("a-svechkov-8_lesson_3_4_dag",
         # schedule_interval='@daily',
         schedule_interval='0 0 * * 1-6',
         # max_active_runs=1,
         default_args=DEFAULT_ARGS,
#   (task 4 amendment)
         # catchup=True,
         tags=['a-svechkov-8']) as dag:

    # pub_ranking_generator_tasks = [
    #     PythonOperator(
    #         task_id=f"pub_{pub_id}",
    #         python_callable=_pub_ranking_generator,
    #         op_kwargs={
    #             "model": pub_id
    #         }
    #     ) for pub_id in ['1', '2', '3']
    # ]
    #
    # choosing_best_pub = BranchPythonOperator(
    #     task_id="choosing_best_pub",
    #     python_callable=_choosing_best_pub
    # )
    #
    # go_to_pub = BashOperator(
    #     task_id="go_to_pub",
    #     bash_command="echo 'go_to_pub'"
    # )
    #
    # go_home = BashOperator(
    #     task_id="go_home",
    #     bash_command=" echo 'go_home'"
    # )

    get_heading = PythonOperator(
        task_id="get_heading",
        python_callable=_get_heading_func,
        op_args=["{{ds.weekday()}}"]
        # op_args=[{{dag_run.logical_date | ds}}]
        # trigger_rule='none_failed',

        # op_kwargs={'execution_date': '{{ execution_date }}'
        #op_kwargs={'date_cur': {{ ds }}}

    )

    # pub_ranking_generator_tasks >> choosing_best_pub >> [go_to_pub, go_home] >> get_heading
    get_heading