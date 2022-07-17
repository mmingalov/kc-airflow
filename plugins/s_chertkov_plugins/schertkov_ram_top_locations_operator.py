import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from s_chertkov_plugins.schertkov_ram_api_hook import SChertkovRickMortyApiHook


class SChertkovRamTopLocationsOperator(BaseOperator):
    """
    Extracts a list of most populated locations from R&M series
    """

    template_fields = ('limit',)
    ui_color = "#40c761"

    def __init__(self, limit: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.limit = limit

    def execute(self, context):
        logging.info(f"Getting {self.limit} most populated locations")
        hook = SChertkovRickMortyApiHook(http_conn_id=None)
        locations = hook.get_locations()

        logging.info(f"Successfully got {len(locations)} locations!")

        top_locations = sorted(locations, key=lambda l: l['resident_cnt'], reverse=True)[:self.limit]
        logging.info(f"These are my top {self.limit} locations")
        logging.info(top_locations)

        top_locations_serialized = ",".join([str(tuple(x.values())) for x in top_locations])
        return top_locations_serialized




