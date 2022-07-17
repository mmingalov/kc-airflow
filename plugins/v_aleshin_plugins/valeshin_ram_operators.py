import requests
import logging
from dataclasses import dataclass

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


@dataclass(frozen=True, order=True)
class Location:
    resident_cnt: int
    id: int
    name: str
    type: str
    dimension: str


class RamTop3LocationOperator(BaseOperator):

    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context) -> None:
        locations_list = []
        for i in range(1, self._get_page_count() + 1):
            response = requests.get(self.url + f'/?page{str(i)}')
            if response.status_code == 200:
                results = response.json().get('results')
                t = [Location(id=el['id'],
                              name=el['name'],
                              type=el['type'],
                              dimension=el['dimension'],
                              resident_cnt=len(el['dimension'])) for el in results]
                locations_list.extend(t)
            else:
                logging.warning(f'HTTP STATUS {response.status_code}')
                raise AirflowException(f'Error in load page {str(i)}')
            locations_list.sort(reverse=True)
            logging.info(f'xcom_push: {locations_list[0:3]}')
            context['ti'].xcom_push(value=locations_list[0:3], key='rick_and_morty_top_3_location')

    def _get_page_count(self) -> int:
        """
        Get count of pages in API
        :return: page count
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            page_count = response.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning(f'HTTP STATUS {response.status_code}')
            raise AirflowException('Error in load page count')


