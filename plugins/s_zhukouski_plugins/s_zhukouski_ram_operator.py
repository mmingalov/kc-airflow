import logging
import requests

from airflow import AirflowException
from airflow.models import BaseOperator


class SZhukouskiRickAndMortyTopLocationOperator(BaseOperator):
    """
    Get top locations
    """

    template_fields = ('top',)
    ui_color = "#e0ffff"

    def __init__(self, top: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top = top

    @staticmethod
    def get_page_count(api_url):

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
        url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        items = []
        for page in range(self.get_page_count(url.format(pg='1'))):
            r = requests.get(url.format(pg=str(page + 1)))
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
                raise AirflowException('Error in load from API')

        result = sorted(items, key=(lambda x: x.get('resident_cnt')), reverse=True)[:self.top]

        logging.info(f'TOP {self.top}')
        for item in result:
            logging.info(item)

        self.xcom_push(context, key='result', value=result)

