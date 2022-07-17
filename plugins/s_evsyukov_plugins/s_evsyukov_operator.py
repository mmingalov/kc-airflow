import requests
import logging
import json

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SevyukovInitiateGP(BaseOperator):

    ui_color = "#e0ffff"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_name = 'students.public.s_evsjukov_ram_location'

    def execute(self, context):
        """
        Initiating table in greenplum
        """
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS {} 
                   (
                        id integer PRIMARY KEY,
                        name varchar(512),
                        type varchar(512),
                        dimension varchar(512),
                        resident_cnt integer
                   )
                   DISTRIBUTED BY (id);
                   """.format(self.db_name))
        cursor.execute("""TRUNCATE TABLE {}""".format(self.db_name))
        conn.close()


class SevsyukovLoadAndSaveOperator(BaseOperator):

    ui_color = "#e0ffff"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.url = 'https://rickandmortyapi.com/api/location'
        self.url_data = None
        self.db_name = 'students.public.s_evsjukov_ram_location'

    def load_data(self):
        """
        Load data from URL
        """
        r = requests.get(self.url)
        locations = []
        if r.status_code == 200:
            json_data = json.loads(r.text)
            locations = json_data['results']
        else:
            logging.warning("Error {}".format(r.status_code))

        lst = []
        for lctn in locations:
            lst.append({'id': lctn['id'],
                        'name': lctn['name'],
                        'type': lctn['type'],
                        'dimension': lctn['dimension'],
                        'resident_cnt': len(lctn['residents'])
                        })
        self.url_data = sorted(lst, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:3]

    def execute(self, context):
        """
        save data to GP
        """
        self.load_data()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        values = []
        for val in self.url_data:
            values.append(f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_cnt']})")

        # logging.info(f"""INSERT INTO {} VALUES {",".join(values)}""".format(self.db_name, ))
        # cursor.execute(f"""INSERT INTO {} VALUES {",".join(values)}""".format(self.db_name))
        logging.info(f'''INSERT INTO {self.db_name} VALUES {','.join(values)}''')
        cursor.execute(f'''INSERT INTO {self.db_name} VALUES {','.join(values)}''')
        conn.commit()
