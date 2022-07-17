
from airflow.models import BaseOperator
import logging
from s_izrantsev_6_plugins.hooks.s_izrantsev_6_ram_hook import IzrantsevRickAndMortyHook


class IzrantsevRickAndMortyOperator(BaseOperator):
    """

    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_locations_tuples(self, locations_results: list) -> list:
        """

        """
        locations_tuples = list()

        for loc in locations_results:
            loc_id = loc.get('id')
            loc_name = loc.get('name')
            loc_type = loc.get('type')
            loc_dimension = loc.get('dimension')
            loc_resident_cnt = len(loc.get('residents'))

            locations_tuple = (loc_id, loc_name, loc_type, loc_dimension, loc_resident_cnt)
            locations_tuples.append(locations_tuple)

            output = 'id: {};name: {};type: {};dimension: {};resident_cnt: {}'

            logging.info("===============================================")
            logging.info(output.format(*locations_tuple))
            # logging.info("===============================================\n")

        return locations_tuples

    def execute(self, context):
        """

        """
        hook = IzrantsevRickAndMortyHook('dina_ram')

        # url = 'https://rickandmortyapi.com/api/location?pjage={pg}'
        locations = list()
        for page in range(hook.get_loc_pages()):
            logging.info("======================================================================================")
            logging.info(f' *** Rick & Morty locations PAGE {page + 1} *** ')
            logging.info("======================================================================================\n")
            results = hook.get_results(str(page + 1))
            locations += self.get_locations_tuples(results)

        final = sorted(locations, key=lambda i: i[-1], reverse=True)[:3]
        return ','.join(map(str, final))
