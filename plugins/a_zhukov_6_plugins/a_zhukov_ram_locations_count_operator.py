import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class ZhukovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
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


class ZhukovRamLocationsCountOperator(BaseOperator):
    """
    Count number of residents in locations
    On ZhukovRickMortyHook
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_locations_on_page(self, result_json: list) -> list:
        """
        Get count of residents for every location
        :param result_json:
        :return: locations
        """
        locations = []
        for one_location in result_json:
            location = {
                'id': one_location['id'],
                'name': one_location['name'],
                'type': one_location['type'],
                'dimension': one_location['dimension'],
                'count_of_residents': len(one_location['residents'])
            }

            locations.append(location)
        return locations

    def execute(self, context):
        """
        Logging top3 locations by count of residents
        """
        hook = ZhukovRickMortyHook('dina_ram')
        all_locations = []
        for page in range(hook.get_location_page_count()):
            one_page = hook.get_location_page(str(page + 1))
            all_locations.extend(self.get_locations_on_page(one_page))

        top3_locations = sorted(all_locations, key=lambda x: x['count_of_residents'], reverse=True)[:3]

        logging.info(f'Top3 in Rick&Morty {top3_locations}')
        return top3_locations
