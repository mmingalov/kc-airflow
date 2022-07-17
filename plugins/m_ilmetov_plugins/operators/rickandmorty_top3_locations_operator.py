from typing import Any
from airflow.models import BaseOperator
from m_ilmetov_plugins.hooks.rickandmorty_hook import RickAndMortyLocationHook


class RickAndMortyLocationOperator(BaseOperator):

    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


    def execute(self, context: Any) -> None:
        """Push top3 location on residents in XCom.

        Args:
            context (Any): Context.
        """

        hook = RickAndMortyLocationHook('dina_ram')
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