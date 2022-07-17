import pandas as pd
from airflow.models import BaseOperator
from o_voinova_7_plugins.o_voinova_7_ram_hook import OVoinova7RickMortyHook
from airflow.hooks.postgres_hook import PostgresHook


class OVoinova7RickMortyOperator(BaseOperator):
    def __init__(self, top_cnt: int, **kwargs):
        super().__init__(**kwargs)
        self._top_cnt = top_cnt

    def execute(self, context):
        conn = PostgresHook(postgres_conn_id='conn_greenplum_write')
        rows = self._get_data().astype(str).to_records(index=False)
        conn.insert_rows(table='o_voinova_7_ram_location', rows=rows)

    def _get_data(self) -> pd.DataFrame:
        conn = OVoinova7RickMortyHook()
        df = conn.get_all_locations()[[
            'id',
            'name',
            'type',
            'dimension',
            'residents'
        ]]
        df['resident_cnt'] = df['residents'].map(len)
        df.drop('residents', axis=1, inplace=True)
        return df.sort_values(
            'resident_cnt',
            ascending=False
        ).iloc[:self._top_cnt, :]
