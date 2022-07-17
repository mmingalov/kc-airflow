from typing import Any
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from a_kravchenko_3_plugins.hooks.ram_hook import RamHook


class Top3LocationsOperator(BaseOperator):
    ui_color = "#ff0000"
    ui_fgcolor = "#000000"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Any):
        hook = RamHook('dina_ram')
        page_count = hook.get_location_page_count()
        locs_count = hook.get_locations_count()
        locs = []

        for i in range(1, page_count+1):
            data = hook.fetch_rows(i)
            for j in range(0, len(data)):
                logging.info(f"write {j} row as a tuple to list")
                row = (
                    data[j]['id'],
                    data[j]['name'],
                    data[j]['type'],
                    data[j]['dimension'],
                    len(data[j]['residents'])
                )
                locs.append(row)
                logging.info(f"row number {j} of {i} page has written")

        if locs_count == len(locs):
            logging.info(f"fetched all locations successfully - {len(locs)}/{locs_count}")
            top3_locations = ','.join(map(str, sorted(locs, key=lambda x: x[-1], reverse=True)[:3]))
            logging.info(f"Top 3 locations are {top3_locations}")
            return top3_locations
        else:
            raise AirflowException(f"rows extracting has failed: {len(locs)}/{locs_count}")
