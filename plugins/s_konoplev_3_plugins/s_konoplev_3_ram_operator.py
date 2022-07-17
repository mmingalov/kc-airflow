import requests
import json
import logging
from airflow.models.baseoperator import BaseOperator

class SKonoplev3Operator(BaseOperator):
    """
        Получает локации "Рик и Морти" с помощью 'https://rickandmortyapi.com/api/location'.
    """

    def __init__(self,  **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):

        result = []
        url = 'https://rickandmortyapi.com/api/location'
        response = requests.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
            for value in data['results']:
                result.append([
                    value['id'],
                    value['name'],
                    value['type'],
                    value['dimension'],
                    len(value['residents']),
                ])
        else:
            logging.warning("status_code: " + response.status_code)

        return result
