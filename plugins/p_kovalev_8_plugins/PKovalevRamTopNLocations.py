from airflow.models import BaseOperator
from airflow.utils.context import Context
from typing import List, Dict, Any


class PKovalevRamTopNLocations(BaseOperator):
    template_fields = ('top_n', 'locations_url')
    ui_color = '#B7223D'
    ui_fgcolor = '#ffffff'

    DEFAULT_LOCATIONS_URL = 'https://rickandmortyapi.com/api/location'

    def __init__(self, top_n: int = 3, locations_url: str = DEFAULT_LOCATIONS_URL, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n: int = top_n
        self.locations_url: str = locations_url

    def execute(self, context: Context) -> List[Dict[str, Any]]:
        import requests
        from airflow.exceptions import AirflowException

        locations: List[Dict[str, Any]] = []
        next_page: str = self.locations_url

        while next_page:

            res = requests.get(next_page)

            if res.status_code == 200:

                res_json: Dict = res.json()

                next_page = res_json['info']['next']
                locations.extend([{'id': loc['id'], 'name': loc['name'], 'type': loc['type'],
                                   'dimension': loc['dimension'],
                                   'resident_cnt': len(loc['residents'])} for loc in res_json['results']])

            else:
                raise AirflowException("Error in load from Rick&Morty API")

        locations.sort(key=lambda el: el['resident_cnt'], reverse=True)

        return locations[:self.top_n]
