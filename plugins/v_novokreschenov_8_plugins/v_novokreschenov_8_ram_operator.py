import requests
import json
import logging
from airflow.models.baseoperator import BaseOperator


class VNovokreschenov8Operator(BaseOperator):
    """
    Получаем локации RAM.
    """
    def __init__(self,  **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        result = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                data = json.loads(r.text)
                for value in data['results']:
                    result.append([
                        value['id'],
                        value['name'],
                        value['type'],
                        value['dimension'],
                        len(value['residents']),
                    ])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return result
