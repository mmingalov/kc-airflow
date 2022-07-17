'''
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.+

С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

> hint
* Для работы с GreenPlum используется соединение 'conn_greenplum_write'

* Можно использовать хук PostgresHook, можно оператор PostgresOperator

* Предпочтительно использовать написанный вами оператор для вычисления top-3 локаций из API

* Можно использовать XCom для передачи значений между тасками, можно сразу записывать нужное значение в таблицу

* Не забудьте обработать повторный запуск каждого таска: предотвратите повторное создание таблицы, позаботьтесь об отсутствии в ней дублей

'''


from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from s_fesenko_3_plugins.s_fesenko_3_rickandmorty_operator import RAM_loc_operator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'end_date': days_ago(1),
    'owner': 's-fesenko-3',
    'poke_interval': 600
}

with DAG("lesson_7", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-fesenko-3']
          ) as dag:

    start = DummyOperator(task_id='start')

## Создание таблицы

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='conn_greenplum_write',
        database = "karpovcourses",
        sql = """create table if not exists s_fesenko_3_ram_location (
                id int PRIMARY KEY,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt int) distributed by (id)""",
        autocommit=True
    )

    xcom_push_ram_top3_location = RAM_loc_operator(
        task_id='xcom_push_ram_top3_location'
    )

    load_top3_locations_gp = PostgresOperator(
        task_id='load_top3_locations_gp',
        postgres_conn_id='conn_greenplum_write',
        database = "karpovcourses",
        sql=[
            "TRUNCATE TABLE s_fesenko_3_ram_location",
            "INSERT INTO s_fesenko_3_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_ram_top3_location') }}",
        ],
        autocommit=True
    )

    end = DummyOperator(task_id='end')

start >> create_table >> xcom_push_ram_top3_location >> load_top3_locations_gp >> end