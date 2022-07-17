from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

import requests
import pandas as pd

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Karpov',
    'poke_interval': 600
}
with DAG("v-prokofev-3-RAM",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['prokofev']
         ) as dag:
    start = DummyOperator(task_id='start')

    def get_location_residents_():
        r=requests.get('https://rickandmortyapi.com/api/location')
        pages_cnt=r.json().get('info').get('pages')
        print(pages_cnt)
        list_res=[]
        for p in range(pages_cnt):

            page = requests.get('https://rickandmortyapi.com/api/location?page='+str(p+1))

            for l in page.json().get('results'):
                id=l.get('id')
                name=l.get('name')
                type=l.get('type')
                dimension=l.get('dimension')
                residents_cnt=0
                for resident in l.get('residents'):
                    residents_cnt+=1
                list_res.append((id,name,type,dimension,residents_cnt))

        df=pd.DataFrame(list_res,columns=['id','name','type','dimension','residents_cnt'])
        df=df.sort_values('residents_cnt',ascending=False)

        df[:3].to_csv('/tmp/v-prokofev-3-RAM.csv',sep=',',header=False,index=False)

    get_location_residents = PythonOperator(
        task_id='get_location_residents',
        python_callable=get_location_residents_,
        dag=dag
    )


    def load_to_gp_():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY v_prokofev_3_ram_location FROM STDIN DELIMITER ','", '/tmp/v-prokofev-3-RAM.csv')


    load_to_gp = PythonOperator(
        task_id='load_to_gp',
        python_callable=load_to_gp_,
        dag=dag
    )

    start >> get_location_residents >> load_to_gp
