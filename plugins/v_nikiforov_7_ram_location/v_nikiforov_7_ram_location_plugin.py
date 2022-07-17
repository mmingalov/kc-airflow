import requests
import logging
from typing import List


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class TopLocationsOperator(BaseOperator):
    """
    Select TOP3 locations
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_locations_on_page(self, result_json: list) -> List:
        """
        Get locations from current page
        """
        locations = []
        for one_char in result_json:
            locations.append(
                {
                    "id": one_char.get("id"),
                    "name": one_char.get("name"),
                    "type": one_char.get("type"),
                    "dimension": one_char.get("dimension"),
                    "resident_cnt": len(one_char.get("residents")),
                }
            )
        return locations

    def get_all_locations(self) -> List:
        """
        Get locations from all pages
        """
        locations = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations.extend(self.get_locations_on_page(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return locations

    def get_top_locations(self, locations: List, n_top_locations: int) -> List:
        """
        Get top locations by residents count
        """
        locations.sort(key=lambda x: x.get('resident_cnt'), reverse=True)
        return locations[:n_top_locations]

    def write_to_gp(self, locations: List) -> None:
        """
        Writing from list to table
        """
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info('Truncating table')
        cursor.execute('TRUNCATE table public."v_nikiforov_7_ram_location"')
        logging.info('Inserting rows')
        cursor.executemany(
            """
            INSERT INTO public."v_nikiforov_7_ram_location"(id, name, type, dimension, resident_cnt)
            VALUES (%(id)s, %(name)s, %(type)s, %(dimension)s, %(resident_cnt)s)
            """,
            locations
        )
        conn.commit()

    def execute(self, context):
        """
        Writing TOP3 locations in Rick&Morty to db
        """
        locations = self.get_all_locations()
        top_locations = self.get_top_locations(locations, 3)
        self.write_to_gp(top_locations)
        print(context['dag'].dag_id)
        print(context['task'].task_id)
