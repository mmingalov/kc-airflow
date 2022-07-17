import requests
import logging
import json

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SevjukovInitiateGP(BaseOperator):

    ui_color = "#e0ffff"

    def __init__(self, db_name: str, **kwargs):
        super().__init__(**kwargs)
        self.db_name = db_name

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


# class SevsjukovLoadFromURLOperator(BaseOperator):
#
#     ui_color = "#e0ffff"
#
#     def __init__(self, url: str, **kwargs):
#         super().__init__(**kwargs)
#         self.locations = None
#         self.url = url
#
#     def load_data(self):
#         """
#         Load data from URL
#         """
#         r = requests.get(self.url)
#         if r.status_code == 200:
#             json_data = json.loads(r.text)
#             self.locations = json_data['results']
#         else:
#             logging.warning("Error {}".format(r.status_code))
#
#     def execute(self, **kwargs):
#         """
#         Save data to XCom
#         """
#         lst = []
#         for lctn in self.locations:
#             lst.append({'id': lctn['id'],
#                         'name': lctn['name'],
#                         'type': lctn['type'],
#                         'dimension': lctn['dimension'],
#                         'resident_cnt': len(lctn['residents'])
#                         })
#         url_data = sorted(lst, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:2]
#         if lst:
#             kwargs['ti'].xcom_push(value=url_data, key='url_data')
#
#
# class SevsjukovQueryOperator(BaseOperator):
#
#     ui_color = "#e0ffff"
#
#     def __init__(self, db_name: str, **kwargs):
#         super().__init__(**kwargs)
#         self.db_name = db_name
#
#     def execute(self, **kwargs):
#         """
#         Insert data from XCom to GP
#         """
#         data = kwargs['ti'].xcom_pull(task_ids='load_data', key='url_data')
#         pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()
#
#         values = []
#         for val in data:
#             values.append(f"({val['id']}, '{val['name']}', {val['type']}', "
#                           f"'{val['dimension']}', '{val['resident_cnt']})")
#
#         # logging.info(f"""INSERT INTO {} VALUES {",".join(values)}""".format(self.db_name, ))
#         # cursor.execute(f"""INSERT INTO {} VALUES {",".join(values)}""".format(self.db_name))
#         logging.info(f"""INSERT INTO {self.db_name} VALUES {",".join(values)}""")
#         cursor.execute(f"""INSERT INTO {self.db_name} VALUES {",".join(values)}""")
#         conn.commit()
