import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class SPorohin3RaMLocationResidentTopOperator(BaseOperator):
    """
    Count location residents
    """

    ui_color = "#e0ffff"

    def __init__(self, resident_top_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.resident_top_count = resident_top_count

    def execute(self, context):
        """
        Using GraphQL API Rick&Morty to minimize internet traffic
        """
        ram_url = 'https://rickandmortyapi.com/graphql'

        def __graph_ql(gql: str):
            """
            GraphQL Request with error handling
            """
            logging.info('request:', gql)
            gql_response = requests.post(ram_url, json={"query": gql})
            if gql_response.status_code == 200:
                logging.info(f'response:', gql_response.text)
                return gql_response.json()
            else:
                logging.warning(f"HTTP STATUS {gql_response.status_code}\n{gql_response.text}")
                raise AirflowException('Error in load from Rick&Morty API')

        # Get pages
        pages_response = __graph_ql("query {locations(page:1) {info { pages }}}")
        pages = pages_response['data']['locations']['info']['pages']
        logging.info(f'RaM locations page count: {pages}')
        # Read data for filtering
        residents_and_location_list = []
        for page in range(1, pages+1):
            logging.info(f'RaM locations page {page} parsing')
            response = __graph_ql("""
                    query {
                        locations(page: #page#) {
                            results { id residents { id } }
                        }
                    }
                    """.replace('#page#', str(page))
                                  )
            for location in response['data']['locations']['results']:
                residents_and_location_list.append((len(location['residents']), location['id']))
        # Data filtering
        residents_and_location_list.sort(reverse=True)
        residents_and_location_list = residents_and_location_list[:self.resident_top_count]
        # Read required data
        top_locations = []
        for resident_cnt, location_id in residents_and_location_list:
            logging.info(f'RaM getting location {location_id} data')
            location_response = __graph_ql("""
                    query {
                        location(id:#loc#) {id, name, type, dimension}}
                    """.replace('#loc#', location_id)
            )
            location_data = location_response['data']['location']
            location_data['resident_cnt'] = resident_cnt
            top_locations.append(location_data)
        return top_locations
