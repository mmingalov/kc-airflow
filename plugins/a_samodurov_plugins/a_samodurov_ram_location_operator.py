import logging
import pandas as pd
import requests
import json
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TopLocationsSamodurovOperator(BaseOperator):
    def __init__(self, top: int = 3, table_name: str = 'a_samodurov_ram_location', **kwargs):
        """
        :param top: Число строк к выводу
        :param table_name: Имя таблицы для записи в БД
        """
        super().__init__(**kwargs)
        self.table_name = table_name
        self.top = top

    def _get_api_json(self) -> json:
        response = requests.get('https://rickandmortyapi.com/api/location').text
        js = json.loads(response)['results']
        logging.info(js)
        return js

    def _json_to_df(self):
        df = pd.DataFrame.from_records(self._get_api_json())
        df['resident_cnt'] = df['residents'].apply(lambda x: len(x))
        df = df[[col for col in df.columns if col not in ('created', 'url', 'residents')]].sort_values(
            by=['resident_cnt'],
            ascending=False
        ).head(self.top)
        logging.info(df.columns)
        return df

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        df = self._json_to_df()
        tpls = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (self.table_name, cols)
        logging.info(sql)
        cursor = conn.cursor()
        cursor.executemany(sql, tpls)
        cursor.close()
        conn.commit()

