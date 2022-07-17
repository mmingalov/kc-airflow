
import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from operator import itemgetter
from airflow.hooks.postgres_hook import PostgresHook


class MGRRamDataLocationOperator(BaseOperator):
    """
    Count number of dead concrete species
    """
    template_fields = ('arr_res',)

    ui_color = "#e0ffff"

    def __init__(self, arr_res: list, **kwargs) -> None:
        super().__init__(**kwargs)
        self.arr_res = arr_res

    def get_write_func(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук

        conn = pg_hook.get_conn()  # берём из него соединение
        pg_hook.run(
            'create table if not exists m_grushina_6_ram_location (id int, name varchar(100), type varchar(100), dimension varchar(100), resident_cnt int);')  # исполняем sql

        for one_loc in self.arr_res:
            cursor = conn.cursor()  # и именованный (необязательно) курсор
            cursor.execute('select * from m_grushina_6_ram_location where id = %s', (one_loc['id'],))
            query_res = cursor.fetchall()  # полный результат
            if not query_res:
                logging.info('empty res--> insert row')
                #logging.info('insert into m_grushina_6_ram_location values (%s,%s,%s,%s,%s)', (one_loc['id'],one_loc['name'],one_loc['type'],one_loc['dimension'],one_loc['cnt_residents'],))
                cursor_ins = conn.cursor()
                cursor_ins.execute("INSERT INTO m_grushina_6_ram_location (id, name, type, dimension, resident_cnt) VALUES (%s, %s, %s, %s, %s)", (one_loc['id'],one_loc['name'],one_loc['type'],one_loc['dimension'],one_loc['cnt_residents']))
                cursor_ins.close()
            else:
                logging.info(query_res)
            cursor.close()
        conn.commit()
        conn.close

        return query_res


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

    def get_species_count_on_page(self, result_json: list, arr_location: list) -> int:
        """
        Get full locations in self.arr_location
        :param result_json:
        :return: 1
        """
        location = dict()
        for result in result_json:
            location['id'] = result.get('id')
            location['name'] = result.get('name')
            location['type'] = result.get('type')
            location['dimension'] = result.get('dimension')
            location['cnt_residents'] = len(result.get('residents'))
            arr_location.append(location)
            field1 = 'id'
            field2 = 'cnt_residents'
            logging.info(f' id = { location[field1] } cnt_residents = { location[field2]  }')
            location = dict()
        return 1

    def explicit_push_func(self, arr: list, **kwargs):
        kwargs['ti'].xcom_push(value=arr, key='var_array')

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        location_count = 0
        arr_location = list()

        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                location_count += self.get_species_count_on_page(r.json().get('results'), arr_location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        arr_location_sort = list()
        arr_location_sort = sorted(arr_location, key=itemgetter('cnt_residents'), reverse=True)
        #arr_location_sort_res = list()
        self.arr_res.append(arr_location_sort[0])
        self.arr_res.append(arr_location_sort[1])
        self.arr_res.append(arr_location_sort[2])
        logging.info(f'{self.arr_res} ')
        self.get_write_func()


