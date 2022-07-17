"""
Урок 5 Задание (вторая часть)
Доработка дага, созданного на уроке 4.
"""

# Импортируем необходимые библиотеки
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

#Задаем в переменных по умолчанию расписание работы дага.
DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'd-anishin-3',
    'poke_interval': 600
}

with DAG("d-anishin-3_lesson_5_task_2",
         schedule_interval= '0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-anishin-3', 'lesson_5']
         ) as dag:

    def connect_to_db_ext_heading_func(weekday_num):
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("greenplum_connection")  # и именованный (необязательно) курсор
        cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday_num}")  # исполняем sql
        query_res = cursor.fetchall()
        return query_res


    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    connect_to_db_ext_heading = PythonOperator(
        task_id='connect_to_db_ext_heading',
        python_callable=connect_to_db_ext_heading_func,
        op_args=['{{ data_interval_start.weekday()+1 }}', ],
        do_xcom_push=True,
    )

    start >> connect_to_db_ext_heading >> end

dag.doc_md = __doc__

start.doc_md = """Начало работы DAG"""
connect_to_db_ext_heading.doc_md = """Основной процесс работы DAG"""
end.doc_md = """Конец работы DAG"""