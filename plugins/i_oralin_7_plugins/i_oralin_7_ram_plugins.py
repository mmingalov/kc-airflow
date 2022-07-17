import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class IOralin7RickMortyLocationHook(HttpHook):
    """
    Interact with locations in Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class IOralin7TopLocationsByResidentsOperator(BaseOperator):
    """
    Find top 3 locations by residents count
    """

    template_fields = ('top_count',)
    ui_color = "#c7ffe9"

    def __init__(self, top_count=3, **kwargs) -> None:
        super().__init__(**kwargs)
        # Better not to init hooks in here because __init__ is executed by scheduler
        self.ram_hook = None
        self.top_count = top_count

    def get_all_locations(self) -> list:
        # Getting all pages at one because there is not a lot of locations
        all_locations = []
        for page in range(self.ram_hook.get_location_page_count()):
            logging.info(f'GETTING PAGE {page + 1}')
            all_locations += self.ram_hook.get_location_page(str(page + 1))
        return all_locations

    def execute(self, context):
        # "dina_ram" - name of connection with rick and morty URL
        self.ram_hook = IOralin7RickMortyLocationHook('dina_ram')
        all_locations = self.get_all_locations()
        # Just logging
        for loc in all_locations:
            residents_count = len(loc.get("residents"))
            logging.info(f'{loc.get("id")} has {residents_count} residents')
        top3_locations = sorted(all_locations, key=lambda loc: len(loc.get("residents")), reverse=True)[:self.top_count]
        logging.info(f'Top {self.top_count} locations in Rick&Morty be residents are '
                     f'{", ".join([("name: " + str(loc.get("name")) + " id: " + str(loc.get("id"))) for loc in top3_locations])}')

        self.xcom_push(context, value=top3_locations, key='top_locations')
