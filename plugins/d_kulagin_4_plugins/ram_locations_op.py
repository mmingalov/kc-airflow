"""
Walk over API https://rickandmortyapi.com/api/location
"""
import logging
from collections import namedtuple
from typing import Any

from airflow.models import BaseOperator

from d_kulagin_4_plugins.ram_locations_hook import RAMLocationHook


class LocationItem(namedtuple('LocationItem', ('id', 'name', 'type', 'dimension', 'resident_cnt'))):
    __slots__ = ()
    def as_sql_values(self):
        return f"({self.id}, '{self.name}', '{self.type}', '{self.dimension}', {self.resident_cnt})"


class RAMLocationsOp(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = set()

    def _result_to_location_item(self, result):
        return LocationItem(
            id=result['id'],
            name=result['name'],
            type=result['type'],
            dimension=result['dimension'],
            resident_cnt=len(result['residents']),
        )

    def execute(self, context: Any):
        hook = RAMLocationHook('dina_ram')  # got from examples in lecture
        pages_count = hook.get_pages_count()
        logging.info(f'got {pages_count} to proceed')

        for page_num in range(1, pages_count+1):
            logging.info(f'processing page {page_num}')
            for result in hook.get_page_results(page_num):
                self._cache.add(self._result_to_location_item(result))

        # sort items by 'resident_cnt' field
        sorted_cache = sorted(self._cache, key=lambda x: x.resident_cnt, reverse=True)

        return ','.join([sorted_cache[i].as_sql_values() for i in range(3)])
