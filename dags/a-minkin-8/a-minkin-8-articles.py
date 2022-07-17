from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pprint import pprint

default_args = {
    'owner': 'aminkin',
    'depends_on_past': False,
    'email': ['aminkin@cbgr.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'schedule_interval': '0 0 * * 1-6' # Monday-Saturday
}

@dag(
    dag_id='aminkin-articles-dag-v05',
    default_args=default_args,
    description='Dag for gathering articles heading'
)
def aminkin_articles_dag():
    
    @task(
        task_id='get-article-heading',
        # templates_dict={'today': '{{ ds }}'}
    )
    def get_article_heading(**kwargs):
        # pprint(kwargs)
        today = kwargs.get('ds')
        # today = kwargs.get('templates_dict').get('today', None)
        query = f"SELECT heading FROM articles WHERE id = extract(isodow from to_date('{today}','YYYY-MM-DD'))"
        print(query)
        # ----------
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        query_res = cursor.fetchall()  # полный результат
        pprint(query_res)
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        # return one_string

    get_article_heading()

dag = aminkin_articles_dag()