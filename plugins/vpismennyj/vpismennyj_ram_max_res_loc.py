import logging
import heapq
from typing import Set
from airflow.models import BaseOperator
from dataclasses import dataclass, fields
from airflow.hooks.http_hook import HttpHook

@dataclass(frozen=True)
class RAM_Location:
    id: int
    name: str
    type: str
    dimension: str
    resident_cnt: int

    def __eq__(self, other):
        return self.resident_cnt == other.resident_cnt
    def __lt__(self, other):
        return self.resident_cnt < other.resident_cnt

    @classmethod
    def create_from_dict(cls, dict_):
        class_fields = {f.name for f in fields(cls)}
        return RAM_Location(**{k: v for k, v in dict_.items() if k in class_fields})


class RAM_Hook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"
    def RAM_Loc_page_count(self) -> int:
        return self.run("api/location").json()["info"]["pages"]
    def RAM_Locs(self, page_num: int) -> list:
        return self.run(f"api/location", data={"page": page_num}).json()["results"]


class RAM_max_res_loc(BaseOperator):
    def __init__(self, top_length: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_length = top_length
    def execute(self, context):
        hook = RAM_Hook('dina_ram')

        location_info = set()

        for page_num in range(hook.RAM_Loc_page_count()):
            for location in hook.RAM_Locs(page_num + 1):
                location["id"] = location.pop("id")
                location["type"] = location.pop("type")
                location["resident_cnt"] = len(location["residents"])
                location_info.add(RAM_Location.create_from_dict(location))

        max_loc_res = heapq.nlargest(self.top_length, location_info)

        context["ti"].xcom_push(key="max", value=max_loc_res)