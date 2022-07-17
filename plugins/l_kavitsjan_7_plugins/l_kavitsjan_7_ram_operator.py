import requests
import logging
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values


class KavitsjanRamLocationCountOperator(BaseOperator):
    """
    Get top 3 locations
    """
    template_fields = ('top_n', 'conn_id', 'table_name',)
    ui_color = "#e0ffff"

    def __init__(self, top_n, conn_id, table_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n
        self.conn_id = conn_id
        self.table_name = table_name

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info('page_count = {page_count}'.format(page_count=page_count))
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {status}".format(status=r.status_code))
            raise AirflowException('Error in load page count')

    def get_residents_count_on_page(self, result_json: list) -> list:
        """
        get count of residents on page
        """
        results = []
        for i in result_json:
            results.append((i.get('id'), i.get('name'), i.get('type'), i.get('dimension'), len(i.get('residents')),))
        return results

    def get_residents_count_on_all_pages(self) -> list:
        results = []
        ram_location_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_location_url.format(pg='1'))):
            r = requests.get(ram_location_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                results.append(self.get_residents_count_on_page(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {status}".format(status=r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        all_locations = [val for sublist in results for val in sublist]
        return all_locations

    def get_top_n_locations(self, all_locations) -> list:
        return sorted(all_locations, key=lambda x:x[4], reverse=True)[:self.top_n]

    def execute(self, context):
        all_locations = self.get_residents_count_on_all_pages()
        top_locations = self.get_top_n_locations(all_locations)
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            execute_values(cursor,
                           "INSERT INTO {table} (id, name, type, dimension, resident_cnt) VALUES %s".format(
                               table=self.table_name
                           ),
                           top_locations
                           )
            conn.commit()
            cursor.execute(
                'SELECT * FROM {table}'.format(table=self.table_name)
            )
            query_res_after_insert = cursor.fetchall()
            logging.info(
                'OUTPUT after:\n{sel_res}'.format(sel_res=query_res_after_insert)
            )




