"""
Забирает из таблицы GP articles
значение поля heading из строки с id
равным дню недели execution_date (понедельник=1, вторник=2, ...)
и складывает получившиеся значения в XCom.
"""

from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


default_args = {
    'start_date': days_ago(10),
    'owner': 'a-ramazanov-3',
    'wait_for_downstream': True,
}


dag_params = {
    'dag_id': 'a-ramazanov-3_2',
    'catchup': False,
    'default_args': default_args,
    'schedule_interval': '5 0 * * 1-6',
    'tags': ['a-ramazanov-3'],
}


def _xcom_push_select(weekday):

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("select_heading")
    cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
    return cursor.fetchall()



with DAG(**dag_params) as dag:

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    xcom_push_select = PythonOperator(
        task_id='xcom_push_select',
        python_callable=_xcom_push_select,
        op_args=['{{ data_interval_start.weekday()+1 }}',],
        do_xcom_push=True,
    )


    start >> xcom_push_select >> end


dag.doc_md = __doc__

start.doc_md = """Начало DAG'а"""
xcom_push_select.doc_md = """Выгрузка результата запроса GP за соответствующий день недели в xcom airflow"""
end.doc_md = """Конец DAG'а"""
