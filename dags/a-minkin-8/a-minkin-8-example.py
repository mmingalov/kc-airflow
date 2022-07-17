from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys

default_args = {
    'owner': 'aminkin',
    'depends_on_past': False,
    'email': ['aminkin@cbgr.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022,1,1),
    'schedule_interval': '0 2 * * *'
}

@dag(
    dag_id='aminkin-info-dag-v01',
    default_args=default_args,
    description='Dag for exploring server information',
    catchup=False
)
def aminkin_info_dag_v01():
    
    @task
    def print_python_info():
        print("Python version:")
        print(sys.version)
        print("Version info:")
        print(sys.version_info)

    gather_server_info = BashOperator(
        task_id='gather_server_info',
        bash_command='hostname; cat /etc/os-release; free -m; env | grep -i air | sort; ps aux | grep -i celeryd',
    )

    print_python_info() >> gather_server_info

dag = aminkin_info_dag_v01()