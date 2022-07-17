import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations_page_count(self):
        """Returns count of locations page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        """Returns location of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class RamLocationsOperator(BaseOperator):
    """
    Returns locations, filtered by the number of characters, based
    On RickMortyHook
    """

    template_fields = ('top_n',)
    ui_color = "#c7ffe9"

    def __init__(self, top_n: str = 5, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n

    def get_top_n_locations(self, locations: list) -> list:
        """
        Returns top n locations, filtered by the number of characters
        """
        top_n_locations = sorted(locations, key=lambda x: len(x['residents']), reverse=True)[:self.top_n]
        logging.info(f'Top {self.top_n} locations are: {[i["name"] for i in top_n_locations]}')
        return top_n_locations

    def execute(self, context):
        """
        Get locations info from API and push top n locations, filtered by the number of characters into the XCom.
        """
        hook = RickMortyHook('dina_ram')
        locations = []
        for page in range(hook.get_locations_page_count()):
            logging.info(f'PAGE {page + 1}')
            locations.extend(hook.get_location_page(str(page + 1)))
        top_n_locations = self.get_top_n_locations(locations)
        return top_n_locations
