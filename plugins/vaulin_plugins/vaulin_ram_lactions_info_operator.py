import requests
import json

from airflow.models.baseoperator import BaseOperator


class VaulinRamLocationInfoOperator(BaseOperator):
    """
    Собирает информацию о локациях РиМ с подсчетом резидентов
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = 'https://rickandmortyapi.com/api/location'

    def execute(self, context):
        """
        Возвращает информацию о локациях с подсчетом количества резидентов
        """

        result = []
        url = self.url

        while url is not None:
            resp = requests.get(url)
            if resp.status_code == 200:
                data = json.loads(resp.text)
                for loc in data['results']:
                    result.append([
                        loc['id'],
                        loc['name'],
                        loc['type'],
                        loc['dimension'],
                        len(loc['residents']),
                    ])
                url = data['info']['next']
            else:
                url = None

        return result
