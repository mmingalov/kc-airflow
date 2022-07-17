import requests
import json
import logging
from airflow.models import BaseOperator


class RamTopThreeLocations(BaseOperator):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        r = requests.get('https://rickandmortyapi.com/api/location')
        locations = json.loads(r.text)['results']

        location_list = []
        for location in locations:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)

        sorted_key_value = sorted(location_list, key=lambda x: x['resident_cnt'])

        top_3_locations = sorted_key_value[-3:]
        logging.info('Top-3 locations is:')
        logging.info(top_3_locations)
        return top_3_locations
