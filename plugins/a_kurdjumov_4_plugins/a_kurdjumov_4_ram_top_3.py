import logging
import json

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class KurdjumovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_on_page(self, page_num: str) -> list:
        """Returns count of location's residents in API"""
        return self.run(f'api/location?page={page_num}').json()['results']

class KurdjumovRamTopNLocationsOperator(BaseOperator):
    """
    Get top N locations by residents and write it to GP
    """
    template_fields = ('n',)
    ui_color = "#f5f5f5"

    SQL_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS
    a_kurdjumov_4_ram_location(
        id int,
        name varchar(200),
        type varchar(200),
        dimension varchar(200),
        resident_cnt int
    )"""


    def __init__(self, n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n = n

    def get_top_n_locations(self, apihook):
        results={}
        for page in range(apihook.get_loc_page_count()):
            json_res = apihook.get_location_on_page(page+1)
            for loc in json_res:
                k = str({"id":loc["id"], "name":loc["name"], "type":loc["type"], "dimension":loc["dimension"]}).replace("'", "\"")
                results[k] = len(loc['residents'])

        results = {k:v for k,v in sorted(results.items(), key=lambda kv: kv[1], reverse=True)[:self.n]}
        
        logging.info(f'Got top {self.n} locations: {results}')

        return results

    def try_create_table(self, pghook):
        logging.info('Trying to create table, if not exist')
        pghook.run(self.SQL_CREATE_QUERY, False)
        logging.info('Table created')

    def execute(self, context):
        apihook = KurdjumovRickMortyHook('dina_ram')
        pghook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        results = self.get_top_n_locations(apihook)
        self.try_create_table(pghook)
        for k in results:
            d = json.loads(k)
            SQL = f"""INSERT INTO a_kurdjumov_4_ram_location
            SELECT {d['id']} as id, '{d['name']}' as name, '{d['type']}' as type, '{d['dimension']}' as dimension, {results[k]} as resident_cnt
            where NOT EXISTS(SELECT * from public.a_kurdjumov_4_ram_location where id = {d['id']})"""
            pghook.run(SQL, False)
            logging.info(f"Inserted next record: {d['id']}, {d['name']}, {d['type']}, {d['dimension']}, {results[k]} to a_kurdjumov_4_ram_location if it wasn't there")
