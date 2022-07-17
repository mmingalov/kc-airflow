import requests
import json
import logging
from airflow.models.baseoperator import BaseOperator
from pandas import DataFrame
from airflow.hooks.postgres_hook import PostgresHook


class KrupkinaTopLocations(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _load_data(self):
        url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(url)
        if r.status_code == 200:
            data = json.loads(r.text)
            locations = data['results']
            result = []
            for location in locations:
                tmp_result = {'id': location['id'],
                              'name': location['name'],
                              'type': location['type'],
                              'dimension': location['dimension'],
                              'resident_cnt': len(location['residents'])}
                result.append(tmp_result)

            self.result = sorted(result, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:2]

        else:
            logging.warning("Error {}".format(r.status_code))

    def execute(self, context):

        self._load_data()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        values = []
        for val in self.result:
            insert_value = f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_cnt']})"
            values.append(insert_value)

        sql_insert = f'''INSERT INTO "e-krupkina-3_ram_location" VALUES {",".join(values)}'''
        logging.info('SQL INSERT QUERY: ' + sql_insert)
        cursor.execute(sql_insert)
        conn.commit()

