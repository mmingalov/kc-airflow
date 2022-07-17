import logging
import requests
from airflow.models import BaseOperator



class KozhinRickMortyHook:
    """
    Interact with Rick&Morty API
    Return total number of pages with locations data
    Return location data page.
    """

    def __init__(self):
        self.base_url="https://rickandmortyapi.com/api/location"

    def get_char_page_count(self):
        """Return total number of pages with locations data"""
        r = requests.get(self.base_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')
        return page_count

    def get_char_page(self, page_num: str) -> list:
        """Return location data page in API"""
        return requests.get(self.base_url + '/?page=' + str(page_num)).json().get('results')


class KozhinRamTopLocationByResidentsCountOperator(BaseOperator):
    """
    Return Top3 locations by resident count in Rick&Morty
    On KozhinRickMortyHook
    """

    ui_color = "#ffe9c7"

    def __init__(self, top_n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n

    def get_residents_count_in_locations_on_page(self, result_json: list) -> list:
        """
        Get count of residents in each location in one page of location
        :param result_json
        :return: resident count - list of tuples
        """
        residents = []
        for one_page in result_json:
            residents += [(one_page.get('id'),
                           one_page.get('name'),
                           one_page.get('type'),
                           one_page.get('dimension'),
                           len(one_page.get('residents')))]
        logging.info(f'resident_count_on_page = {residents}')
        return residents

    def top3_location_by_resident_count(self, residents_in_locations: list) -> list:
        """
        return top location tuples by residents count (position 4 in tuple ) in residents_in_locations list of tuple
        """
        top = sorted(residents_in_locations, key=lambda x: x[4], reverse=True)[:3]
        return top

    def execute(self, context):
        """
        Return all locations with resident number
        Logging count of residents in Rick&Morty locations
        """
        hook = KozhinRickMortyHook()
        residents_in_all_locations = []
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            residents_in_all_locations += self.get_residents_count_in_locations_on_page(one_page)
        logging.info(f'All locations by resident count in Rick&Morty extracted')
        return (residents_in_all_locations)
