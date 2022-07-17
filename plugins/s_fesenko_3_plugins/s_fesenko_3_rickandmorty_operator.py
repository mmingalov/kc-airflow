from typing import Any
from airflow.models import BaseOperator
from s_fesenko_3_plugins.s_fesenko_3_rickandmorty_http_hook import RAM_loc_hook


class RAM_loc_operator(BaseOperator):
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Any) -> None:

        hook = RAM_loc_hook('dina_ram')
        locations = []
        for location in hook.gen_location_schema():
            row = (
                location['id'],
                location['name'],
                location['type'],
                location['dimension'],
                len(location['residents'])
            )
            self.log.info(tuple(row))
            locations.append(row)

        top3_locations = sorted(locations, key=lambda x: x[-1], reverse=True)[:3]
        return_value = ','.join(map(str, top3_locations))
        self.log.info('Top 3 location on residents:')
        self.log.info(return_value)

        return return_value