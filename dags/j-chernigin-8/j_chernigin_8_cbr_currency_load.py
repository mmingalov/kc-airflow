"""
    Get currencies state from cbr.ru and load to GreenPlum
    Used operators:
        * PythonOperator - logging initial state of variables
        * BashOperator - get data by url and save it to /tmp
"""
from airflow.decorators import task, dag
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

import logging

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'j-chernigin-8',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'poke_interval': 600
}

@dag(
        schedule_interval='@daily',
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=['j-chernigin-8', 'cbr-currency', 'greenplum']
)
def cbr_currency_to_greenplum():
    dag.doc_md = __doc__
    # TODO: Add description (doc_md) to all tasks
    # TODO: Use variable for table and columns
    # TODO: Check data existing in table based on a date from XML
    # TODO: Save to CSV format avoiding XML saving

    xml_data_url = 'https://cbr.ru/scripts/xml_daily.asp?date_req={{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}'
    xml_file_path = '/tmp/j_chernigin_8_cbr_currency_tmp.xml'
    csv_file_path = '/tmp/j_chernigin_8_cbr_currency_tmp.csv'
    gp_connection = 'conn_greenplum'

    def check_data_existing_func(**kwargs):
        query = kwargs['query']
        logging.info('Query that will be executed: {}'.format(query))

        pg_hook = PostgresHook(gp_connection)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            logging.info('The table j_chernigin_cbr_currency_state has results on the DAG\'s start date')
            return False
        else:
            logging.info('The table j_chernigin_cbr_currency_state has not results on the DAG\'s start date')
            return True

    check_data_existing = ShortCircuitOperator(
        task_id='check_data_existing',
        python_callable=check_data_existing_func,
        op_kwargs={
            'query': "select * from karpovcourses.public.j_chernigin_cbr_currency_state where date='{{ ds }}' limit 1"
        }
    )

    # Get currency.xml state from CBR.ru and save it to /tmp
    load_and_save_currency_to_xml = BashOperator(
        task_id='load_and_save_currency_to_xml',
        bash_command='curl -s {url} | iconv -f windows-1251 -t utf-8 > {path}'.format(
            url=xml_data_url,
            path=xml_file_path
        )
    )

    @task
    def convert_xml_to_csv():
        import csv
        import xml.etree.ElementTree as ET
        from datetime import datetime

        parser = ET.XMLParser(encoding='utf-8')
        tree = ET.parse(xml_file_path, parser=parser)
        root = tree.getroot()

        with open(csv_file_path, 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            date = datetime.strftime(datetime.strptime(root.attrib['Date'], '%d.%m.%Y'), '%Y-%m-%d')

            for valute in root.findall('Valute'):
                num_code = valute.find('NumCode').text
                char_code = valute.find('CharCode').text
                nominal = valute.find('Nominal').text
                name = valute.find('Name').text
                value = (valute.find('Value').text).replace(',', '.')
                valute_id = valute.attrib['ID']

                valute_row_to_write = [date] + [valute_id] + [num_code] + [char_code] + [nominal] + [name] + [value]
                writer.writerow(valute_row_to_write)
                logging.info('message="row wrote to csv {}"'.format(csv_file) + ' row="' + ','.join(valute_row_to_write) + '"')

    @task
    def load_csv_to_greenplum():
        pg_hook = PostgresHook(gp_connection)
        query = 'COPY karpovcourses.public.j_chernigin_cbr_currency_state {columns} FROM STDIN {options}'.format(
            columns="(date, valute_id, num_code, char_code, nominal, name, value)",
            options="DELIMITER ','"
        )
        logging.info('PG Command for write data to GreenPlum table: {}'.format(query))
        pg_hook.copy_expert(query, csv_file_path)

    # Remove temporary file after data saving to GreenPlum
    remove_saved_xml_file = BashOperator(
        task_id='remove_saved_xml_file',
        bash_command='rm {} {}'.format(xml_file_path, csv_file_path)
    )


    check_data_existing >> load_and_save_currency_to_xml >> convert_xml_to_csv() >> load_csv_to_greenplum() >> remove_saved_xml_file

dag = cbr_currency_to_greenplum()