import requests
import json
import logging
from airflow.models.baseoperator import BaseOperator


class ShabalovLocationOperator(BaseOperator):
    """
        Собирает локации сериала "Рик и Морти" с количеством резидентов.
    """

    def __init__(self,  **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Информация о резидентах
        """
        result = []
        url = 'https://rickandmortyapi.com/api/location'
        response = requests.get(url)
        if response.status_code == 200:
            logging.info("SUCCESS")
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
            logging.warning("HTTP STATUS {}".format(r.status_code))

        return result

