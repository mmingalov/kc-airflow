import logging
from operator import itemgetter

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class Maverbah4RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """
    def __init__(self, http_conn_id, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method='GET'

    def get_loc_page_count(self):
        """Returns count of pages in API"""
        return self.run('/api/location').json()['info']['pages']

    def get_loc_page(self, page_num):
        """Get the list of locations on page"""
        return self.run(f'/api/location?page={page_num}').json()['results']

class Maverbah4FindLocationsOperator(BaseOperator):
    """
    Find 3 location with maximum residents counts
    """
    ui_color = "#e0ffff"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_num_of_residents_on_page(self, result_json):
        """
        Get the count of residents for each location on page
        """
        num_of_residents_on_page={}
        for loc in result_json:
            num_of_residents_on_page[loc.get('name')]=[loc.get('id'), loc.get('name'),
                                      loc.get('type'),loc.get('dimension'),len(loc.get('residents'))]
        logging.info(num_of_residents_on_page)
        return (num_of_residents_on_page)

    def execute(self, context):
        """
        Logging count of top 3 location
        by residents number
        """
        num_of_residents_dict={}
        residents=[]
        top_3_locations=[]
        hook=Maverbah4RickMortyHook('dina_ram')
        for page in range(hook.get_loc_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_loc_page(str(page + 1))
            for key, value in self.get_num_of_residents_on_page(one_page).items():
                num_of_residents_dict[key]=value
                residents.append(value[4])
        sorted_values=sorted(residents, reverse=True)
        for key,value in num_of_residents_dict.items():
            if value[4] in [sorted_values[0], sorted_values[1], sorted_values[2]]:
                top_3_locations.append(num_of_residents_dict[key])
        top_3_locations=sorted(top_3_locations, key=itemgetter(4), reverse=True)
        logging.info(f'top 3 locations are {top_3_locations}')
        return top_3_locations








