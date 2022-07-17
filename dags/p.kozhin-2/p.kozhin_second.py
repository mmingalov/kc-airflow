"""
DAG урок 4. СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 2
Работает с понедельника по субботу, но не по воскресеньям
Ходит в наш GreenPlum (импользуем соединение 'conn_greenplum')
Забирает из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)
Складывает получившееся значение в XCom
Результат работы виден в интерфейсе с XCom
"""


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import logging


DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'p.kozhin-2',
    'poke_interval': 600
}

dag = DAG("p.kozhin-2_second",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['p.kozhin-2']
          )


def _get_weekday_func(**context):
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_num = context['data_interval_start'].weekday()
    weekday_num_corrected = context['data_interval_start'].weekday()+1
    logging.info("-----------------------------------------------")
    logging.info(f"Date: {context['data_interval_start']}")
    logging.info(f"Weekday: {weekday_num}, Weekday name:{days[weekday_num]}, row_id: {weekday_num_corrected}")
    logging.info("-----------------------------------------------")
    context["task_instance"].xcom_push(key="row_id", value=weekday_num_corrected)  # explicit_push_func


get_weekday = PythonOperator(
    task_id="get_weekday",
    python_callable=_get_weekday_func,
    dag=dag
)


def _get_gp_heading_by_weekday_id_func(templates_dict, **context):  # как вариант -  с использованием templates_dict
    row_id = templates_dict["row_id"]
    logging.info(f"row_id: {row_id}")
    # подключени к Greenplum
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {row_id}')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    # explicit_push_func
    context["task_instance"].xcom_push(key="query_res", value={row_id: query_res})  # можно, например, вернуть dict



get_gp_heading_by_weekday_id = PythonOperator(
    task_id="get_gp_heading_by_weekday_id",
    python_callable=_get_gp_heading_by_weekday_id_func,
    templates_dict={
        "row_id": "{{ task_instance.xcom_pull(task_ids='get_weekday', key='row_id') }}"  # xcom_pull получаем значение для row_id
    },
    dag=dag
)

get_weekday >> get_gp_heading_by_weekday_id

dag.doc_md = __doc__

get_weekday = """Получает значение дня недели выполнения. 
Пишет в XCOM значение row_id = weekday()+1 (Понедельник = 1, Вторник = 2 и т.д.)
Пишет в Log дату запуска, день недели и row_id
"""
get_gp_heading_by_weekday_id = """Обращаемся к базе Greenplum. 
Забирает из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...). 
Передает значение в XCOM"""
