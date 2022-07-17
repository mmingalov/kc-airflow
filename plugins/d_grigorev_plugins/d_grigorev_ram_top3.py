import requests
import logging
import json
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook

class RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_loc_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']


class Ram_Top3_loc(BaseOperator):
    """
    Form json with id, name, type, dimension, resident_cnt from one page
    :param result_json
    return one_page_json
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_loc_one_page(self, result: list) -> list:
        """
        Get count of dead or alive in one page of character
        :param result
        :return: dead_or_alive_count
        """
        loc_list_one_page = []
        for one_char in result:
            loc_list_one_page.append({'id': one_char['id'], 'name': one_char['name'],
                                      'type': one_char['type'], 'dimension': one_char['dimension'],
                                      'resident_cnt': len(one_char['residents'])})
        return loc_list_one_page

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = RickMortyHook('dina_ram')
        loc_list = []
        for page in range(hook.get_loc_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_loc_page(str(page + 1))
            loc_list += self.get_loc_one_page(one_page)
        logging.info(f"Length of loc_list: {len(loc_list)}")
        loc_list.sort(key=lambda x: x["resident_cnt"], reverse=True)
        top_3 = {}
        for value in loc_list[:3]:
            top_3[value['id']] = value
        context['ti'].xcom_push(key='d-grigorev-ram-location', value=top_3)