"""hook to API rick_and_morty"""
import logging

from airflow.providers.http.hooks.http import HttpHook


class RamHook(HttpHook):
    """Interact with Rick & Morty API"""

    def __init__(self, http_conn_id: str = ..., method: str = "GET", **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_location_page_count(self) -> int:
        """Returns count of page in API"""

        logging.info("get count of location page")
        page_cnt = self.run('api/location').json()['info']['pages']
        logging.info(f"Count of location pages is {page_cnt}")
        return page_cnt

    def get_locations_count(self) -> int:
        """Returns count of locations in API"""

        logging.info("get count of locations")
        locations_count = self.run('api/location').json()['info']['count']
        logging.info(f"count of locations is {locations_count}")
        return locations_count

    def fetch_rows(self, page_num: int) -> list:
        """Param: page_num - number of page API

           Returns: list of tuples which is location schema
           - fields, and related values"""

        logging.info(f"fetch rows from page number {page_num}")
        rows = self.run(f'api/location?page={page_num}').json()['results']
        logging.info(f"There has read {len(rows)} rows from {page_num} page")
        return rows
