"""

Забираем данные с Cbr и складываем в Greenplum;;

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from random import randint

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-papka',
    'poke_interval': 600
}

with DAG("ipapka_cbr",
        schedule_interval='0 11 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['i-papka']
        ) as dag:
    # Функция которая всегда верна
    def success():
        pass


    # Функция вычислений;
    def calc():
        n = randint(11, 19) // 10
        k = 8
        res = k * n
        print(f'Result: {res}')
        return f'Result: {res}'


    # Функция которая скипает задачу
    def skip():
        raise AirflowSkipException


    # Функция которая падает с ошибкой
    def failed():
        raise AirflowFailException


    calculation_task = PythonOperator(
        task_id='calculation_task',
        python_callable=calc,
        dag=dag
    )

    skip_task = PythonOperator(
        task_id='skip_task',
        python_callable=skip,
        dag=dag
    )

    failed_task = PythonOperator(
        task_id='failed_task',
        python_callable=failed,
        dag=dag
    )

    dummy_task = DummyOperator(
        task_id='dummy_task',
        trigger_rule='one_success'
    )

    success_task = PythonOperator(
        task_id='success_task',
        python_callable=success,
        dag=dag
    )

    failed_task_2 = PythonOperator(
        task_id='failed_task_2',
        python_callable=failed,
        dag=dag
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello, Dina"',
        dag=dag
    )

    success_task_2 = PythonOperator(
        task_id='success_task_2',
        python_callable=lambda: print("Success"),
        trigger_rule='one_success',
        dag=dag
    )

    today = datetime.today().strftime('%d/%m/%Y')
    load_cbr_xml_script = f"""
    curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={today} | iconv -f Windows-1251 -t UTF-8 > /tmp/ipapka_cbr.xml
    """
    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command=load_cbr_xml_script,
        dag=dag
    )

    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/ipapka_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/ipapka_cbr.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])


    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable= export_xml_to_csv_func)

    create_table_sql_query = """ 
            CREATE TABLE IF NOT EXISTS ipapka_cbr (
            dt text,
            id text,
            num_code text,
            char_code text,
            nominal text,
            nm text,
            value text);
            TRUNCATE TABLE  ipapka_cbr;
        """

    create_table = PostgresOperator(
        sql=create_table_sql_query,
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write"
    )

    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY ipapka_cbr FROM STDIN DELIMITER ','", '/tmp/ipapka_cbr.csv')
    load_csv_to_greenplum = PythonOperator(
        task_id= 'load_csv_to_greenplum',
        python_callable = load_csv_to_greenplum_func
    )

    def select_data_func(**context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute("SELECT ic.* FROM ipapka_cbr ic WHERE ic.nm = 'Евро';")  # исполняем sql

        query_res = cursor.fetchall()  # полный результат
        if query_res:
            context['ti'].xcom_push(value=float(query_res[0][6]), key='query_res')
            logging.info(query_res)
        one_string = cursor.fetchone()  # если вернулось единственное значение
        if one_string:
            context['ti'].xcom_push(value=one_string, key='one_string')
            logging.info(one_string)


    select_data = PythonOperator(
        task_id= 'select_data',
        python_callable = select_data_func,
        provide_context=True
    )
    # вывожу заголовок в зависимости от дня недели
    def select_heading_func(**context):
        day_of_week = datetime.today().weekday()+1
        query = f'SELECT heading FROM articles WHERE id = {day_of_week}'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name2")  # и именованный (необязательно) курсор
        cursor.execute(query)  # исполняем sql

        query_res = cursor.fetchall()  # полный результат
        if query_res:
            context['ti'].xcom_push(value=query_res, key='query_heading')
            logging.info(query_res)
        one_string = cursor.fetchone()  # если вернулось единственное значение
        if one_string:
            context['ti'].xcom_push(value=one_string, key='one_heading')
            logging.info(one_string[0])


    select_heading = PythonOperator(
        task_id='select_heading',
        python_callable=select_heading_func,
        provide_context=True
    )
    def expect_heading_func(**context):
        for i in range(1,8):
            query = f'SELECT heading FROM articles WHERE id = {i}'
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor("named_cursor_name3")  # и именованный (необязательно) курсор
            cursor.execute(query)  # исполняем sql

            query_res = cursor.fetchall()  # полный результат
            if query_res:
                context['ti'].xcom_push(value=query_res, key=f'query_heading_{i}')
                logging.info(query_res)
            one_string = cursor.fetchone()  # если вернулось единственное значение
            if one_string:
                context['ti'].xcom_push(value=one_string, key=f'one_heading_{i}')
                logging.info(one_string[0])


    expect_heading = PythonOperator(
        task_id='expect_heading',
        python_callable=expect_heading_func,
        provide_context=True
    )




[calculation_task, skip_task, failed_task] >> dummy_task >> [success_task, failed_task_2,bash_task] >> success_task_2
success_task_2 >> export_cbr_xml >> xml_to_csv >> create_table >> load_csv_to_greenplum
load_csv_to_greenplum >> select_data
load_csv_to_greenplum >> select_heading
[select_data >> select_heading] >> expect_heading
