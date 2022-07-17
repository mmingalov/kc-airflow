import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class N_Paveleva_Hook(HttpHook):
    """
        Interact with Rick&Morty API.
        """
    def __init__(self, http_conn_id, *args, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, *args, **kwargs)
        self.method = 'GET'

    def get_pages_count(self):
        return self.run('api/location').json()['info']['pages']

    def get_page_results(self, page_num):
        return self.run(f'api/location?page={page_num}').json()['results']

class N_Paveleva_Location_Operator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        hook = N_Paveleva_Hook('dina_ram')
        pages_count = hook.get_pages_count()
        logging.info(f'{pages_count} pages')
        locations = []

        for page_num in range(1, pages_count + 1):
            logging.info(f'processing page {page_num}')
            for result in hook.get_page_results(page_num):
                locations.append({
                    'id': result['id'],
                    'name': result['name'],
                    'type': result['type'],
                    'dimension': result['dimension'],
                    'resident_cnt': len(result['residents'])
                })

        top_locations = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[0:3]
        logging.info(f'top locations: {top_locations}')
        self.xcom_push(context, key='top3_locations', value=top_locations)
        return top_locations
