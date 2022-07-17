import logging
import requests
import json
from typing import Any, Dict, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class JDylko3RaMLocationsOperator(BaseOperator):
    """
        Gets top-3 locations with max residents and pushes results into XCom.
    """

    template_ext = ('xcom_key',)
    ui_color = '#C8E7FF'

    def __init__(self, xcom_key: str = 'Locations', **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_key = xcom_key
        self.host = 'https://rickandmortyapi.com/'
        self.session = requests.Session()

    @staticmethod
    def _check_response(response: requests.Response) -> None:
        """Checks response and raises Airflow exception in case of HTTP Error"""

        if not (response.status_code == requests.codes.ok):
            logging.error(f'HTTP Error: {response.reason}')
            logging.error(f'Response text: {response.text}')
            raise AirflowException(f'{response.status_code}: {response.reason}')

    def _run(self, endpoint: str) -> Union[Dict[Any, Any], None]:
        """Performs request and returns response json"""

        req = requests.Request(method='GET', url=f'{self.host}{endpoint}')
        prep_req = self.session.prepare_request(req)
        settings = self.session.merge_environment_settings(prep_req.url, {}, None, None, None)
        settings.update({'timeout': 10, 'verify': False})
        logging.info(f'Sending GET request to url {self.host}{endpoint}')
        try:
            response = self.session.send(prep_req, **settings)
            self._check_response(response)
            logging.info(f'GET request to url {self.host}{endpoint} is SUCCESSFUL')
            return response.json()
        except requests.exceptions.ConnectionError as e:
            logging.warning(f'Connection error while sending GET request to {self.host}{endpoint}: {e}')
            raise e

    def _get_page_count(self) -> Union[int, None]:
        """Returns number of pages with locations"""
        return self._run('api/location')['info']['pages']

    def _get_page(self, page: Optional[int] = 1) -> Union[list, None]:
        """Return json for a given page from API"""
        return self._run(f'api/location?page={page}')['results']

    @staticmethod
    def _get_locations_from_page(data: Union[list, None]) -> Union[list, None]:
        """Gets list of locations data from page"""
        if not data:
            return None
        locations = []
        for location in data:
            locations.append(
                {
                    'id': location.get('id'),
                    'name': location.get('name'),
                    'type': location.get('type'),
                    'dimension': location.get('dimension'),
                    'resident_cnt': len(location.get('residents'))
                }
            )
        return locations

    def execute(self, context):
        """Gets top-3 locations by residents count and pushes them into XCom"""
        total_pages = self._get_page_count()
        all_locations = []
        for page in range(1, total_pages + 1):
            logging.info(f'Parsing page {page} of {total_pages}')
            all_locations += self._get_locations_from_page(self._get_page(page))

        top3_locations = json.dumps(sorted(all_locations, key=lambda x: x['resident_cnt'], reverse=True)[:3])
        logging.info(f'Top 3 locations by resident count are: {top3_locations}')
        self.xcom_push(context, self.xcom_key, top3_locations)

