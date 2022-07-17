
import requests
import logging
import json


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class KotsjubaRamResidentCountOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    template_fields = ('species_type',)
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
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
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def execute(self, context):
        logging.info("execute")
        locations = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                json_answer_text = json.loads(r.text)
                res = json_answer_text["results"]
                for item in res:
                    locations.append({
                        "id": item["id"],
                        "name": item["name"],
                        "type": item["type"],
                        "dimension": item["dimension"],
                        "resident_cnt": len(item["residents"])

                    })
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        locations_sorted = sorted(locations, key=lambda x: x["resident_cnt"], reverse=True)[:3]
        locations_str = ""
        for item in locations_sorted:
            locations_str += "(" + str(item["id"]) + ",'" + item["name"] + "'" + ",'" + item["type"] + "'" + ",'" + \
                             item["dimension"] + "'" + "," + str(item["resident_cnt"]) + "),"
        locations_str = locations_str[:-1]

        context['ti'].xcom_push(value=locations_str, key='top3_locations')

        return locations_str

