import logging

import requests
from airflow import AirflowException
from airflow.models import BaseOperator


class GualexRamTopLocationOperator(BaseOperator):
    """
    Get top locations of Rick&Morty with maximun residents
    """

    template_fields = ('top_len',)
    ui_color = "#87ceeb"

    def __init__(self, top_len: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_len = top_len

    @staticmethod
    def get_page_count(api_url):
        """
        Get count of page in API
        """
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
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        items = []
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')

                for location in r.json()['results']:
                    item = {
                        'id': location['id'],
                        'name': location['name'],
                        'type': location['type'],
                        'dimension': location['dimension'],
                        'resident_cnt': len(location['residents'])
                    }
                    logging.info(item)
                    items.append(item)

            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        top = sorted(items, key=(lambda x: x.get('resident_cnt')), reverse=True)[:self.top_len]

        logging.info(f'TOP {self.top_len}')
        for item in top:
            logging.info(item)

        self.xcom_push(context, key='top', value=top)
