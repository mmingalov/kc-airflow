import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class JanovskajaRAMLocationsOperator(BaseOperator):
    '''
    Counts top n locations with maximal number of residents
    '''
    template_fields = ('locations_number',)
    ui_color = '#bee8e0'

    def __init__(self, locations_number: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.locations_number = locations_number

    @staticmethod
    def get_page(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning("HTTP STATUS {}".format(response.status_code))
            raise AirflowException('Error in load from Rick&Morty API')

    @staticmethod
    def _process_locations(location_dict):
        location_dict['residents'] = len(location_dict['residents'])
        location_dict.pop('url')
        location_dict.pop('created')

        return location_dict

    def _get_locations_from_page(self, page):
        locations_dict = page.get('results')
        return [self._process_locations(location) for location in locations_dict]

    def _get_top_locations(self, url):
        current_page = self.get_page(url.format(pg='1'))
        locations = self._get_locations_from_page(current_page)
        total_pages = current_page.get('info').get('pages')

        for page in range(2, total_pages + 1):
            page = self.get_page(url.format(pg=str(page)))
            locations.extend(self._get_locations_from_page(page))

        self._locations = sorted(locations, key=lambda x: x['residents'])[-self.locations_number:]

    def execute(self, context):
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        self._get_top_locations(api_url)

        logging.info('Top locations are:', self._locations)
        return self._locations
