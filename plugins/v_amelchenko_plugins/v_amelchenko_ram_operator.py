"""
simple test dag
"""

import json
import requests
import logging


from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


API_URL = 'https://rickandmortyapi.com/api/location'

class VladRickMortyOperator(BaseOperator):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def top_location(self):

        r = requests.get(API_URL)

        location = json.loads(r.text)['results']

        total_list = []
        for loc in location:
            loc_dict = {
                'id': loc['id'],
                'name': loc['name'],
                'type': loc['type'],
                'dimension': loc['dimension'],
                'resident_cnt': len(loc['residents'])
            }
            total_list.append(loc_dict)

        result = sorted(total_list, key=lambda x: x['resident_cnt'], reverse=True)[:3]

        return result



    def execute(self, context):

        result = self.top_location()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        insert_values = []
        for val in result:
            insert_value = f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_cnt']})"
            insert_values.append(insert_value)

        sql_insert = f'''INSERT INTO "v_amelchenko_ram_top" VALUES {",".join(insert_values)}'''
        logging.info('QUERY: ' + sql_insert)
        cursor.execute(sql_insert)
        conn.commit()
