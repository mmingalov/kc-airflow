from airflow.models.baseoperator import BaseOperator
import requests
import logging

class RickAndMortyTopLocationsOperator(BaseOperator):

    base_url="https://rickandmortyapi.com/api/"
    location_url=base_url+"location/"

    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def __init__(self, top_limit: int, **kwargs) -> None:
        super().__init__(**kwargs)
        # locations limit
        self.top_limit = top_limit

    def execute(self, context):
        """
        Gather top N locations with most residents into sql insert query
        """
        logging.info(f"Gathering {self.top_limit} locations")
        top_locations = [] # array of records (location, residents count, name, type, dimension)
        location_pages = self.get_page_count(self.location_url)
        for page in range(location_pages):
            r = requests.get(self.location_url + f"?page={str(page + 1)}")
            if r.status_code == 200:
                page_results = r.json().get('results')
                logging.info(f'There are {len(page_results)} locations on {str(page + 1)}')
                for loc in page_results:
                    top_locations.insert(0,(loc.get('id'), len(loc.get('residents')), loc.get('name'), loc.get('type'), loc.get('dimension')))
            else:
                logging.info("HTTP STATUS {}".format(r.status_code))
                raise AirflowException(f'Error in load from Rick&Morty API locations page {str(page + 1)}')

        # sort locations array by residents cnt desc
        top_locations.sort(key=lambda tup: tup[1], reverse=True)

        # gather top locations in string for insert query
        sql_values = ''
        for i in range(min(self.top_limit,len(top_locations))):
            if sql_values != '':
                sql_values = sql_values + ',\n'
            sql_values = sql_values + f"({top_locations[i][0]},'{top_locations[i][2]}','{top_locations[i][3]}','{top_locations[i][4]}',{top_locations[i][1]})"

        return sql_values