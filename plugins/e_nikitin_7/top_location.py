import json
from unittest import result
import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

class RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['results']


class TopLocation(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_top_location(locations, top=3):
        locations = sorted(locations, key=lambda x: len(x['residents']), reverse=True)[:top]
        result = [] 
        for l in locations:
            result.append(f"({l['id']}, '{l['name']}', '{l['type']}', '{l['dimension']}', {len(l['residents'])})")
        
        return result
        

    def execute(self, context):
        hook = RickMortyHook('dina_ram')
        
        result = hook.get_location()
        
        top_location = self.get_top_location(result)
       
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        sql_statement = f'''INSERT INTO "e-nikitin-7_ram_location" VALUES {",".join(top_location)}'''
        cursor.execute(sql_statement)
        conn.commit()
