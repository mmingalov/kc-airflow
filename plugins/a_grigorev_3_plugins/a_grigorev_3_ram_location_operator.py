import requests
import logging

from airflow import AirflowException
from airflow.models import BaseOperator


def get_locations(url):
    response = requests.get(url)
    response_json = response.json()

    next_page = response_json['info']['next']
    locations = []
    columns = ['id', 'name', 'type', 'dimension', 'residents']
    for location in response_json['results']:
        locations.append({x: location[x] if x != 'residents' else len(location[x]) for x in columns})

    if next_page is None:
        return locations
    else:
        locations.extend(get_locations(next_page))
        return locations


def sort_by_residents_cnt(location):
    return location['residents']


def get_top3_locations(locations):
    locations.sort(key=sort_by_residents_cnt, reverse=True)
    return locations[:3]


class RAMLocationOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        locations_url = 'https://rickandmortyapi.com/api/location'
        # context['ti'].xcom_push(value=top3_locations, key='top3_locations')
        return get_top3_locations(get_locations(locations_url))
