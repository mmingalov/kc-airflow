import logging
import heapq
from dataclasses import dataclass, fields
from typing import Set

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


@dataclass(frozen=True)
class RickAndMortyLocation:
    _id: int
    name: str
    _type: str
    dimension: str
    resident_cnt: int

    def __eq__(self, other):
        return self.resident_cnt == other.resident_cnt

    def __lt__(self, other):
        return self.resident_cnt < other.resident_cnt

    @classmethod
    def create_from_dict(cls, dict_):
        class_fields = {f.name for f in fields(cls)}
        return RickAndMortyLocation(**{k: v for k, v in dict_.items() if k in class_fields})

class MaskaevRickMortyHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_locations_count_page(self) -> int:
        return self.run("api/location").json()["info"]["pages"]

    def get_locations(self, page_num: int) -> list:
        return self.run(f"api/location", data={"page": page_num}).json()["results"]


class MaskaevTopLocationOperator(BaseOperator):

    def __init__(self, top_length: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_length = top_length

    def execute(self, context):
        hook = MaskaevRickMortyHook('dina_ram')

        locations_data = set()

        for page_num in range(hook.get_locations_count_page()):
            for location in hook.get_locations(page_num + 1):
                location["_id"] = location.pop("id")
                location["_type"] = location.pop("type")
                location["resident_cnt"] = len(location["residents"])

                locations_data.add(RickAndMortyLocation.create_from_dict(location))

        locations_top_n = heapq.nlargest(self.top_length, locations_data)

        context["ti"].xcom_push(key="top", value=locations_top_n)
