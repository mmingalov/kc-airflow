from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    'start_date':datetime(2022, 3, 1),
    'end_date':datetime(2022, 3, 15),
    'owner': 'j-kiunov',
    'poke_interval': 600,
    'schedule_interval':'0 0 * * 1-6'}
@dag(
    dag_id='kin_less_3_4',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['kin']
)
def generate_dag_kiunov():
    #Получаем данные из GreenPlum за соответствующий день
    @task(do_xcom_push = True)
    def get_data_from_gp():
        context = get_current_context()
        ds = context["ds"]
        today = datetime.strptime(ds, '%Y-%m-%d').weekday()+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today}')
        db_result = cursor.fetchone()[0]
        #query_res = cursor.fetchall()
        return db_result
    get_data_from_gp()
kiunov_dag = generate_dag_kiunov()

def some_function_in_your_library():
    context = get_current_context()
    ti = context["ti"]

#itsok
