import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values


class ChudakovaOperator(BaseOperator):
    template_fields = ('conn_id', 'table_name',)

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def get_page_count(self, api_url):
        r = requests.get(api_url)

        if r.status_code == 200:
            logging.info("SUCCESS CONNECTION")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top3_locations(self):
        api_url = 'https://rickandmortyapi.com/api/location'

        all_results = []
        for i in range(1, self.get_page_count(api_url)):
            url = f'https://rickandmortyapi.com/api/location?page={i}'
            r = requests.get(url)
            data = r.json()
            all_results.extend(data['results'])

        sorted_file_content = sorted(all_results, key=lambda x: len(x['residents']), reverse=True)[:3]

        top3_locations = []
        for i in sorted_file_content[:3]:
            top3_locations.append(
                (i.get('id'), i.get('name'), i.get('type'), i.get('dimension'), len(i.get('residents')),))
        return top3_locations

    def execute(self, context):
        top3_locations = self.get_top3_locations()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, f"INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s", top3_locations)
        conn.commit()
        cursor.execute(f'SELECT * FROM {self.table_name}')
        query_res_after_insert = cursor.fetchall()
        logging.info(f'OUTPUT after:\n{query_res_after_insert}')
