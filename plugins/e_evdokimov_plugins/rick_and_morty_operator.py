import json
import logging

import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class TopLocationsRnM(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_api_data(self):

        r = requests.get('https://rickandmortyapi.com/api/location')
        json_answer_text = json.loads(r.text)
        self.locations = json_answer_text['results']

    def _get_top_locations(self, top=3):

        result = []
        for location in self.locations:
            location_res = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            result.append(location_res)

        self.result = sorted(result, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:top]

    def execute(self, context):

        self._get_api_data()
        self._get_top_locations()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        list_to_insert = []
        for item in self.result:
            list_of_items = f"({item['id']}, '{item['name']}', '{item['type']}', '{item['dimension']}', {item['resident_cnt']})"
            list_to_insert.append(list_of_items)

        sql_insert = f'''INSERT INTO "e-evdokimov_ram_location" VALUES {",".join(list_to_insert)}'''
        logging.info('SQL INSERT QUERY: ' + sql_insert)
        cursor.execute(sql_insert)
        conn.commit()
