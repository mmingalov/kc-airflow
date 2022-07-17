"""
Загружаем в базу данных три локации сериала Рик и Морти с наибольшим количеством резидентов
"""

import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class SivashTopFromRickAndMorty(BaseOperator):

    def __init__(self, table_name: str = 'g_sivash_4_ram_location', **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name

    def get_page_count(self, api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info('Success connect!')
            page_count = r.json().get('info').get('pages')
            logging.info(f'Readed pages count: {page_count}')
            return int(page_count)
        else:
            logging.info(f'Bad request, error {r.status_code}')
            raise AirflowException('Error in load page count')

    def execute(self, context):

        url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        result_df = pd.DataFrame()

        for page in range(self.get_page_count(url.format(pg='1'))):
            r = requests.get(url.format(pg=page + 1))
            if r.status_code == 200:
                results = r.json().get('results')
                df = pd.DataFrame(results)
                result_df = result_df.append(df)
                logging.info(f'Read pages {page + 1}: ok')
            else:
                logging.info('Error in read pages')
                raise AirflowException('Error in read pages')

        result_df['resident_count'] = result_df['residents'].apply(len)
        result_df = result_df.sort_values('resident_count', ascending=False).head(3)

        hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = hook.get_conn()
        cur = conn.cursor()

        insert_values = []
        for i, val in result_df.iterrows():
            insert_value = f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_count']})"
            insert_values.append(insert_value)

        cur.execute(f'INSERT INTO {self.table_name} (id, name, type, dimension, resident_count) VALUES {",".join(insert_values)};')
        conn.commit()

