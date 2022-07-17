import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class RamTop3LocationOperator(BaseOperator):
    """
    Поиск топ 3-х локаций по количеству резидентов
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        r = requests.get('https://rickandmortyapi.com/api/location')
        if r.status_code == 200:
            logging.info('success read ' + 'https://rickandmortyapi.com/api/location')
            jsonLocations = r.json().get('results')
            locations = []
            for location in jsonLocations:
                locations.append({'name': location['name'], 'type': location['type'],
                                  'dimension': location['dimension'], 'resident_cnt': len(location["residents"])})

            locations.sort(key=lambda x: x['resident_cnt'], reverse=True)
            context['ti'].xcom_push(value=locations[0:3], key='RickAndMorty_Top3_Location')
            # return locations
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in read https://rickandmortyapi.com/api/location')
            pass
