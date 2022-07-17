import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class MSamsonovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def location_pages_count(self):
        """Page count from API"""
        return self.run('api/location').json()['info']['pages']

    def locations(self, page_num: int) -> list:
        """Locations by page number"""
        return self.run(f'api/location?page={page_num}').json()['results']


class MSamsonovRAMLocationOperator(BaseOperator):
    """
    Top 3 locations by resident count
    """

    template_fields = ('cnt',)
    ui_color = "#c7ffe9"

    def __init__(self, cnt: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cnt = cnt

    def execute(self, context):
        """
        Get top locations by resident count and save to xcom
        """
        hook = MSamsonovRickMortyHook('dina_ram')

        locations = []
        for page_idx in range(hook.location_pages_count()):
            logging.info(f'Page {page_idx + 1}')
            one_page_locations = hook.locations(page_idx + 1)
            locations += one_page_locations

        for location in locations:
            location['resident_cnt'] = len(location['residents'])

        top_n_locations = sorted(locations, key=lambda loc: loc['resident_cnt'], reverse=True)[:self.cnt]

        fields_needed = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        result = [{key: location[key] for key in fields_needed} for location in top_n_locations]

        logging.info(f'Top locations {self.cnt}: {result}')
        context["ti"].xcom_push(value=result, key="msamsonov_ram_locations")
