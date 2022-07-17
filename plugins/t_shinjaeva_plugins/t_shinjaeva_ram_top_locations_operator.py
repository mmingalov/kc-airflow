import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException

class TShinjaevaRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/character').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/character?page={page_num}').json()['results']

class TShinjaevaRamTopLocationsOperator(BaseOperator):
    """
    Return top <n> Locations by <sort_parameter>
    """
    template_fields = ('top_count', 'sort_parameter')

    def __init__(self, top_count: int = 3, sort_parameter: str = 'resident_cnt', **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_count = top_count
        self.sort_parameter = sort_parameter

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
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_locations_on_page(self, result_json: list) -> list:
        locations = []
        for res in result_json:
            location = {
                'id': res['id'],
                'name': res['name'],
                'type': res['type'],
                'dimension': res['dimension'],
                'resident_cnt': len(res['residents'])
            }
            locations.append(location)
        return locations

    def top_locations(self, locations: list) -> list:
        return sorted(locations, key=lambda i: i[self.sort_parameter], reverse=True)[0:self.top_count]

    def execute(self, context) -> list:
        locations = []
        ram_char_url = "https://rickandmortyapi.com/api/location?page={pg}"
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page+1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations = locations + self.get_locations_on_page(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        context['task_instance'].xcom_push(value=self.top_locations(locations), key='top_locations')
        return self.top_locations(locations)
