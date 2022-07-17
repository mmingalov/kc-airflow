# 5 модуль, 4 урок
# ДАГ выполняет следующие шаги:
# 01 Launch_Day_xCom- запись в Xcom дня запуска ДАГА. При запуске в воскресение (07.11.2021 ) exec_day почему то 5
# 02 Launch_Day - Проверка дня запуска. Реализовано через ShortCircuitOperator чтобы потренироваться. Через расписание конечно проще.
# 03 GetDataFromGreenplum - Складываем в  xCom заселекченую строчку из Greenplum


from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import datetime
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'romashkin',
    'poke_interval': 600
}

dag = DAG("a.romashkin_greenplum_to_XCom_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov', 'greenplum', 'XCom']
          )


def Not_Sunday(execution_dt):
    exec_day = datetime.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    if exec_day not in [6]: return True
    else: return False


Launch_Day = ShortCircuitOperator(
    task_id='Launch_Day',
    python_callable=Not_Sunday,
    op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
    dag=dag
)

def GetDataFromGreenplumFunc(execution_dt):
    # 01. Определяем день запуска ДАГ - а
    exec_day = datetime.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()

    # 02. Подключаемся к Greenplum и селектим heading
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute('SELECT heading FROM articles WHERE id = ' + str(exec_day))  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    # one_string = cursor.fetchone()[0]  если вернулось единственное значение
    # 03. Складываем получившееся значение в XCom
    return query_res


GetDataFromGreenplum = PythonOperator(
    task_id='GetDataFromGreenplum',
    python_callable=GetDataFromGreenplumFunc,
    op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
    dag=dag
)

# Шаг добавлен для проверки работы функции Launch_Day. 
def Launch_Day_xComFunc(execution_dt):
    exec_day = datetime.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    return exec_day

Launch_Day_xCom = PythonOperator(
    task_id='Launch_Day_xCom',
    python_callable=Launch_Day_xComFunc,
    op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
    dag=dag
)

start_sensor = TimeDeltaSensor(
    task_id='start_sensor',
    delta=timedelta(seconds=6*60*60),
    dag=dag
)

start_sensor >> Launch_Day_xCom>> Launch_Day >> GetDataFromGreenplum