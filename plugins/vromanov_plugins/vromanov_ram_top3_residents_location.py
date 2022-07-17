import logging

from operator import attrgetter
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class RAMResponse:
    def __init__(self, id, name, type, dimension, residents_cnt):
        self.id = id
        self.name = name
        self.type = type
        self.dimension = dimension
        self.residents_cnt = residents_cnt

    def __repr__(self):
        return repr((self.id, self.name, self.type, self.dimension, self.residents_cnt))


class RomanovRAMHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        return self.run(f'api/location?page={page_num}').json()['results']


class RomanovRAMTopLocationOperator(BaseOperator):

    def __init__(self, dead_or_alive: str = 'Dead', **kwargs) -> None:
        super().__init__(**kwargs)
        self.dead_or_alive = dead_or_alive

    def execute(self, context):
        """
        Using 'dina_ram' Airflow connection id to get RAM host
        """
        hook = RomanovRAMHook('dina_ram')

        ram_locations = list()
        for page in range(hook.get_location_page_count()):
            for location in hook.get_location_page(page + 1):
                resp = RAMResponse(location["id"], location['name'], location['type'], location['dimension'],
                                   len(location['residents']))
                ram_locations.append(resp)
            logging.info(f'Locations list for page {page + 1} : {ram_locations}')

        ram_locations = sorted(ram_locations, key=attrgetter('residents_cnt'), reverse=True)

        context["ti"].xcom_push(key="ram_top_locations", value=ram_locations[:3])
