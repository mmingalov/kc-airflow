import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from a_shumilov_4_plugins.hooks.shumilov_rick_and_morty_hooks import ShumilovRickMortyHook


class ShumilovCreateDatabaseOperator(BaseOperator):
    """
    Create DB for RAM
    """
    REQUEST = """CREATE TABLE if not exists 
    shumilov_ram_location (
    id int,
    name varchar(100), 
    type varchar(100), 
    dimension varchar(100), 
    resident_cnt int
    )"""

    ui_color = "#66334f"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def create_database(self):
        """
        function for create database
        """

        pg_hook = PostgresHook("conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(self.REQUEST)
        connection.commit()
        cursor.close()
        connection.close()


    def execute(self, context):
        """

        """
        self.create_database()
        logging.info(f'Create database')


class ShumilovInsertDataOperator(BaseOperator):
    """
    Insert data from site RAM to database
    """
    SQL_INSERT = """
   insert into shumilov_ram_location
   select * from json_populate_recordset(null::json_type, '{}')
   where id not in (select id from shumilov_ram_location);
    """

    ui_color = "#430505"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def insert_data(self):
        """
        function for download and insert data
        """

        ram_hook = ShumilovRickMortyHook('dina_ram')
        list_residents = ram_hook.return_n_locations_max_residents(3)

        pg_hook = PostgresHook("conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        logging.info('[' + ','.join(list_residents) + ']')
        logging.info(self.SQL_INSERT.format('[' + ','.join(list_residents) + ']'))
        cursor.execute(self.SQL_INSERT.format('[' + ','.join(list_residents) + ']'))
        connection.commit()
        cursor.close()
        connection.close()


    def execute(self, context):
        """

        """
        self.insert_data()
        logging.info(f'Insert data to database')
