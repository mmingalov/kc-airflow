from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
import logging
import requests
import pandas as pd


class LocationOperator(BaseOperator):
    """
    Get location with top number of residents
    """
    api_url = 'https://rickandmortyapi.com/api/location'
    template_fields = ('top_location_count', 'CSV_PATH',)
    ui_color = "#e0ffff"

    def __init__(self, top_location_count: int = 3, CSV_PATH: str = '/tmp/top_location.csv', **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_location_count = top_location_count
        self.CSV_PATH = CSV_PATH

    def get_df_location(self):
        """
        Get df info of location in API
        :param api_url
        :return: df
        """
        r = requests.get(self.api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            location_data = []
            for row in r.json()['results']:
                location_data.append(dict(
                    id=row['id'],
                    name=row['name'],
                    type=row['type'],
                    dimension=row['dimension'],
                    residents=len(row['residents'])))
            df = pd.DataFrame(location_data)
            df = df.astype({'id': 'int64'})
            df.drop(columns='type', inplace=True)
            logging.info(df)
            return df
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top_loc_resident_count(self, df):
        """
        Get location with top number of residents
        :param top_count
        :return: df with top locations by resident count
        """
        return df.sort_values(ascending=False, by='residents').head(self.top_location_count)

    def execute(self, context):
        """
        Logging df location with top number of residents
        @param context:
        @return:
        """
        df = self.get_df_location()
        df_sorted = self.get_top_loc_resident_count(df)
        logging.info(f'Top {self.top_location_count} location with number of residents: \n{df_sorted}')
        df_sorted.to_csv(self.CSV_PATH, index=False, header=False)
