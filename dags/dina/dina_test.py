"""
Простейший даг.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}

dag = DAG("dina_test",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=8,
          tags=['karpov']
          )


def get_string_for_today_func(**kwargs):
    today = kwargs['templates_dict']['today']  # 2022-02-17
    day_of_week = datetime.strptime(today, '%Y-%m-%d').weekday() + 1

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    cursor = pg_hook.get_conn().cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
    return cursor.fetchall()[0][0]


get_string_for_today = PythonOperator(
    task_id='get_string_for_today',
    python_callable=get_string_for_today_func,
    templates_dict={'today': '{{ ds }}'},
    dag=dag
)


def write_3_max_location_func(**kwargs):
    # Локации забираются единственным запросом, без деления на страницы
    r = requests.get('https://rickandmortyapi.com/api/location')

    # Конвертируем в json, запоминаем только значимую часть ответа
    locations = json.loads(r.text)['results']

    # Соберём локации в массив словарей
    location_list = []
    for location in locations:
        location_dict = {
            'id': location['id'],
            'name': location['name'],
            'type': location['type'],
            'dimension': location['dimension'],
            'resident_cnt': len(location['residents'])
        }
        location_list.append(location_dict)

    # Отсортируем этот массив и выберем три первых элемента
    sorted_locations = sorted(location_list,
                              key=lambda cnt: cnt['resident_cnt'],
                              reverse=True)
    top3_location = sorted_locations[:3]

    # Соберём список значений для вставки в таблицу
    insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                     for loc in top3_location]

    # И добавим эти значения в таблицу
    insert_sql = f"INSERT INTO dina_ram_location VALUES {','.join(insert_values)}"
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    pg_hook.run(insert_sql, False)


write_3_max_location = PythonOperator(
    task_id='write_3_max_location',
    python_callable=write_3_max_location_func,
    dag=dag
)

