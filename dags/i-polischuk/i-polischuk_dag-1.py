"""
Даг:
-> 0. Скачивание архива с датасетом из облака
-> 1. Разархивирование файлов
-> 2. Удаление файла архива
-> 3. Вывести список файлов в датасете
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'i-polischuk',
    'poke_interval': 600
}

with DAG("dataset_preparing",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-polischuk']
         ) as dag:

    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command='curl -O https://filedn.com/lAv70NoRi0O8LpU8BzbIRXJ/GitHubProgrammingLanguagesData.zip',
        dag=dag
    )

    unzip_dataset = BashOperator(
        task_id='unzip_dataset',
        bash_command='unzip GitHubProgrammingLanguagesData.zip',
        dag=dag
    )

    remove_zip = BashOperator(
        task_id='remove_zip',
        bash_command='rm GitHubProgrammingLanguagesData.zip',
        dag=dag
    )

    list_files = BashOperator(
        task_id='list_files',
        bash_command='ls -la -p',
        dag=dag
    )

    download_dataset >> unzip_dataset >> remove_zip >> list_files
