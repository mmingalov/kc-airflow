from __future__ import annotations
import logging
import pendulum
from typing import NamedTuple
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class s_schelokovskij_6_RickMortyHook(HttpHook):
    """
    Interacts with Rick&Morty API
    """

    # 'https://rickandmortyapi.com/api/location'
    def __init__(self, http_conn_id: str, endpoint: str) -> None:
        HttpHook.__init__(self, method='GET', http_conn_id=http_conn_id)
        self.endpoint = endpoint

    def get_all_locations(self) -> list[Location]:
        """
        Returns all locations.
        """
        locations = []
        results = self.run(self.endpoint).json()['results']
        for loc in results:
            locations.append(
                Location(
                    id=loc['id'],
                    name=loc['name'],
                    type=loc['type'],
                    dimension=loc['dimension'],
                    resident_cnt=len(loc['residents']),
                )
            )
        return locations


class Location(NamedTuple):
    id: int
    name: str
    type: str
    dimension: str
    resident_cnt: int

    def get_sql_values_clause(self) -> str:
        return f"({self.id},'{self.name}','{self.type}','{self.dimension}',{self.resident_cnt})"


class s_schelokovskij_6_RamTop3LocationsOperator(BaseOperator):
    """
    Finds top 3 locations with greatest number of residents.
    """

    def __init__(self, **kwargs) -> None:
        BaseOperator.__init__(self, **kwargs)

    def execute(self, context):
        data_load_hook = s_schelokovskij_6_RickMortyHook(
            http_conn_id='',
            endpoint='https://rickandmortyapi.com/api/location'
        )

        locations = data_load_hook.get_all_locations()
        locations = sorted(
            locations,
            key=lambda loc: loc.resident_cnt,
            reverse=True,
        )
        top3_locations: list[Location] = locations[0:3]

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(
            """
            CREATE TABLE IF NOT EXISTS s_schelokovskij_6_ram_location(
                id INTEGER PRIMARY KEY,
                name TEXT,
                type TEXT,
                dimension TEXT,
                resident_cnt INTEGER
            )
            DISTRIBUTED BY (id);
            """
        )
        pg_hook.run(
            f"""
            DELETE FROM s_schelokovskij_6_ram_location
            WHERE id in ({','.join(str(loc.id) for loc in top3_locations)})
            """
        )

        values_sql_clause = 'VALUES' + ', '.join(
            loc.get_sql_values_clause() for loc in top3_locations
        )
        pg_hook.run(
            f"""
            INSERT INTO s_schelokovskij_6_ram_location (id, name, type, dimension, resident_cnt)
            {values_sql_clause}
            """
        )


ram_dag = DAG(
    'shchelokovskiy_ram_dag',
    schedule_interval='@Hourly',
    max_active_runs=1,
    tags=['shchelokovskiy_dag_ram_locations'],
    start_date=pendulum.datetime(2022, 3, 29, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 29, tz="UTC"),
)
ram_dag.doc_md = 'Contains a task to find top 3 locations with greatest number of residents.'

find_top3_locations_operator = s_schelokovskij_6_RamTop3LocationsOperator(
    task_id='shchelokovskiy_find_top3_locations_operator',
    dag=ram_dag,
)
find_top3_locations_operator.doc_md = 'Finds top 3 locations with greatest number of residents.'
find_top3_locations_operator
