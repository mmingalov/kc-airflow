import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

class IpapkaRamLocationCountOperator(BaseOperator):
    """
    Count number of dead concrete species
    """


    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


    def get_location_count(self, api_url: str) -> int:
        """
        Get count of locations in API
        :param api_url
        :return: locations count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            loc_count = r.json().get('info').get('count')
            logging.info(f'location_count = {loc_count}')

            return int(loc_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load locations count')


    def get_location_values(self, result_json: dict) -> list:
        """
        Get values from location
        :param result_json:
        :return: list_of_values
        """
        list_of_names = ['id', 'name', 'type', 'dimension', 'residents']
        list_of_values = []
        for name in list_of_names:
            if name == 'residents':
                list_of_values.append(len(result_json.get(name)))
            else:
                list_of_values.append(result_json.get(name))

        logging.info(f'Values from location = {list_of_values}')
        return list_of_values

    def insert_values_func(self, dct: dict) -> None:
        # dct = context['ti'].xcom_pull(key='result', task_ids='execute_task')
        logging.info(dct)
        values = [(k, v[0], v[1], v[2], v[3]) for k, v in dct.items()]
        logging.info(values)
        query = 'INSERT INTO "i-papka_ram_location"(id, name, type, dimension, resident_cnt) VALUES (%s,%s,%s,%s,%s);'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.executemany(query, values)  # исполняем sql
        conn.commit()


    def execute(self, context):
        dct = {}
        url = 'https://rickandmortyapi.com/api/location/'
        for num in range(1, self.get_location_count(url) + 1):
            location_url = 'https://rickandmortyapi.com/api/location/{num}'
            r = requests.get(location_url.format(num=num))
            result_json = r.json()
            list_of_values = self.get_location_values(result_json)
            dct[list_of_values[0]] = list_of_values[1:]
        dct = {k: v for k, v in sorted(dct.items(), key=lambda x: x[1][3], reverse=True)[:3]}
        # logging.info(dct)
        # context['ti'].xcom_push(value=dct, key='result')
        self.insert_values_func(dct)
