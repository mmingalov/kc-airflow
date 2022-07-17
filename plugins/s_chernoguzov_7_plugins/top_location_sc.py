import json
from unittest import result
import requests
import logging
import heapq

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class TopLocation(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(url)
        response = json.loads(r.text)
        range_t = response['info']['pages']
        df = []

        for i in range(range_t):
            r = requests.get(url)
            response = json.loads(r.text)
            url = response['info']['next']
            for l in response['results']:
                df.append([len(l['residents']), l['id'], l['name'], l['type'], l['dimension']])
        insert = heapq.nlargest(3, df)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        sql_statement = f'''insert into public.s_chernoguzov_7_ram_location (resident_cnt, id, name, type, dimension) VALUES (%s,%s,%s,%s,%s); '''
        cursor.executemany(sql_statement, insert)
        conn.commit()
