"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти"
с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

>hints
* Для работы с GreenPlum используется соединение 'conn_greenplum_write' в случае, если вы работаете с LMS
либо настроить соединение самостоятельно в вашем личном Airflow. Параметры соединения:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: students (не karpovcourses!!!)
Login: student
Password: Wrhy96_09iPcreqAS

* Можно использовать хук PostgresHook, можно оператор PostgresOperator
* Предпочтительно использовать написанный вами оператор для вычисления top-3 локаций из API
* Можно использовать XCom для передачи значений между тасками, можно сразу записывать нужное значение в таблицу
* Не забудьте обработать повторный запуск каждого таска:
    предотвратите повторное создание таблицы,
    позаботьтесь об отсутствии в ней дублей
"""

import json
import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class TopLocationsRickAndMorty(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_api_data(self):

        # Get data from API
        r = requests.get('https://rickandmortyapi.com/api/location')

        # Convert it to python dictionary
        json_answer_text = json.loads(r.text)

        # Get necessary key
        self.locations = json_answer_text['results']

    def _get_top_locations(self, top=3):

        result = []  # empty list for result
        for location in self.locations:
            location_res = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            result.append(location_res)

        # Sort list of dictionaries by desc
        self.result = sorted(result, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:top]

    def execute(self, context):

        # Get data from API
        self._get_api_data()

        # Get top locations
        self._get_top_locations()

        # Create connection to GreenPlum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Prepare values for inserting
        insert_values = []
        for val in self.result:
            insert_value = f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_cnt']})"
            insert_values.append(insert_value)

        # Save to XCOM values for insert
        sql_insert = f'''INSERT INTO "m-mingalov_ram_location" VALUES {",".join(insert_values)}'''
        logging.info('SQL INSERT QUERY: ' + sql_insert)
        cursor.execute(sql_insert)
        conn.commit()
