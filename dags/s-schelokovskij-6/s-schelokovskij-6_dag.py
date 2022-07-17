"""
Dags to perform some tasks against books and words.
"""
import logging
import csv
from typing import Iterable, NamedTuple
import requests

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator

from pendulum import datetime
from collections import OrderedDict


class BookAttrs(NamedTuple):
    book_id = 'book_id'
    title = 'title'
    authors = 'authors'
    average_rating = 'average_rating'
    isbn = 'isbn'
    isbn13 = 'isbn13'
    language_code = 'language_code'
    num_pages = 'num_pages'
    ratings_count = 'ratings_count'
    text_reviews_count = 'text_reviews_count'
    publication_date = 'publication_date'
    publisher = 'publisher'


class WordAttrs:
    word = 'word'
    pos = 'pos'
    freq = 'freq'


def show(headers: Iterable, dict: dict):
    sep = ','
    headers = sep.join(headers)
    logging.info(headers)
    count = 0
    for k, v in dict.items():
        logging.info(f'{k}{sep}{v}')
        count += 1
        if count == 10:
            break


def load_data_file(url: str):
    response = requests.get(url)
    file_generator = (
        line.decode(encoding='utf-8')
        for line in response.iter_lines(decode_unicode=True)
    )
    return file_generator


def get_reader(url: str):
    books_generator = load_data_file(url)
    reader = csv.DictReader(
        books_generator,
        delimiter=',',
    )
    return reader


def find_book_with_biggest_ratings_count():
    books_reader = get_reader(
        'https://storage.yandexcloud.net/backet-lab/airflow/books.csv'
    )

    title_groups: dict[str, int] = {}
    for book_record in books_reader:
        current_ratings_count = int(book_record[BookAttrs.ratings_count])
        title = book_record[BookAttrs.title]
        if BookAttrs.title in title_groups:
            if current_ratings_count > title_groups[title]:
                title_groups[title] = current_ratings_count
        else:
            title_groups[title] = current_ratings_count
    sorted_book_groups = OrderedDict(
        sorted(title_groups.items(), key=lambda book_group: book_group[0])
    )

    show(
        [BookAttrs.title, BookAttrs.ratings_count],
        sorted_book_groups,
    )


def find_how_many_books_in_each_language():
    books_reader = get_reader(
        'https://storage.yandexcloud.net/backet-lab/airflow/books.csv'
    )

    language_code_groups: dict[str, int] = {}
    for book_record in books_reader:
        language_code = book_record[BookAttrs.language_code]
        if language_code in language_code_groups:
            language_code_groups[language_code] += 1
        else:
            language_code_groups[language_code] = 0
    sorted_language_code_groups = OrderedDict(
        sorted(language_code_groups.items(), key=lambda group: -group[1])
    )

    show(
        [BookAttrs.language_code, 'number_of_books'],
        sorted_language_code_groups,
    )


# Create books DAG
books_dag = DAG(
    'shchelokovskiy_books_dag',
    schedule_interval='@Hourly',
    max_active_runs=1,
    tags=['shchelokovskiy_dag_books'],
    catchup=False,
    start_date=datetime(2022, 3, 29, tz="UTC"),
)
books_dag.doc_md = 'Performs some tasks against books.'

ratings_count_task = PythonOperator(
    task_id='find_book_with_biggest_ratings_count',
    python_callable=find_book_with_biggest_ratings_count,
    dag=books_dag,
)
ratings_count_task.doc_md = 'finds books that have the biggest count of ratings'

books_count_per_language_task = PythonOperator(
    task_id='books_count_per_language_task',
    python_callable=find_how_many_books_in_each_language,
    dag=books_dag,
)
books_count_per_language_task.doc_md = 'counts number of books in each language'

wait_for_books_dag_task = ExternalTaskSensor(
    task_id="wait_for_books_dag_task",
    external_dag_id=books_dag.dag_id,
    external_task_id=books_count_per_language_task.task_id,
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)


def find_words_containing_specific_letter_at_position(
    letter: str,
    position: int,
):
    words_reader = get_reader(
        'https://storage.yandexcloud.net/backet-lab/airflow/words.csv'
    )
    words_with_count: dict[str, int] = {}
    for word_record in words_reader:
        word: str = word_record[WordAttrs.word]
        try:
            if word.index(letter) == position:
                if word in words_with_count:
                    words_with_count[word] += 1
                else:
                    words_with_count[word] = 1
        except ValueError:
            pass
    show(
        [WordAttrs.word, 'count'],
        words_with_count,
    )


# Create words DAG
words_dag = DAG(
    'shchelokovskiy_words_dag',
    schedule_interval='@Hourly',
    max_active_runs=1,
    tags=['shchelokovskiy_dag_words'],
    catchup=False,
    start_date=datetime(2022, 3, 29, tz="UTC"),
)
words_dag.doc_md = 'Perform some tasks against words.'

find_words_containing_letter_task = PythonOperator(
    task_id='find_words_containing_letter_task',
    python_callable=find_words_containing_specific_letter_at_position,
    dag=books_dag,
    op_args=['в', 3],
)
find_words_containing_letter_task.doc_md = 'counts number of books in each language'

# pipeline
ratings_count_task >> books_count_per_language_task >> wait_for_books_dag_task >> find_words_containing_letter_task
