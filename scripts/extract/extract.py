import os
import logging
import time
from typing import Callable

import requests
import psycopg2
from psycopg2 import sql


def max_id_in_table(cursor) -> int:
    """Вернуть максимальный id поста в таблице raw_users_by_posts.

    Args:
        cursor: курсор psycopg2.

    Returns:
        Максимальный id поста в таблице raw_users_by_posts.
    """
    cursor.execute('select max(id) from raw_users_by_posts')
    records = cursor.fetchall()
    
    max_id_in_table = records[0][0]
    max_id_in_table = 0 if max_id_in_table is None else max_id_in_table
    return max_id_in_table


def retry(max_retries: int) -> Callable:
    """Декоратор для ретраев функции.

    Args:
        max_retries: максимальное кол-во ретраев.

    Returns:
        Декоратор.
    """
    def retry_decorator(func: Callable) -> Callable:
        def _wrapper(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except:
                    time.sleep(1)
            
            result = func(*args, **kwargs)
            return result
        return _wrapper
    return retry_decorator


@retry(5)
def read_posts(url: str) -> list[dict]:
    """Загрузить данные из URL API.

    Args:
        url: URL API.

    Returns:
        Данные полученные по URL API.
    """
    result = requests.get(url)
    assert result.status_code == 200, f'Server get error code {result.status_code}'

    result = result.json()
    return result


def data_filter(posts: list[dict], max_id: int) -> list[dict]:
    """Очистка данных от записей, которые были загруженны ранее.

    Args:
        posts: изначальные данные.
        max_id: максимальный id поста в таблице raw_users_by_posts.

    Returns:
        Очищенные от данные.
    """
    result = [
        post
        for post in posts
        if post['id'] > max_id
    ]
    return result


def load_into_postgress(posts: list[dict], cursor) -> None:
    """Загрузка данных в СУБД Postgress.

    Args:
        posts: очищенные данные.
        cursor: курсор psycopg2.

    Returns:
        Очищенные от данные.
    """
    values = []
    for post in posts:
        values.append((post['userId'], post['id'], post['title'], post['body']))

    insert_script = sql.SQL(
        'INSERT INTO raw_users_by_posts (user_id, id, title, body) VALUES {}'
    ).format(
        sql.SQL(',').join(map(sql.Literal, values))
    )
    cursor.execute(insert_script)


def main() -> None:
    logging.info('extract: Connect to PostgreSQL.')
    connect = psycopg2.connect(
        dbname=os.environ['POSTGRES_DB'], 
        user=os.environ['POSTGRES_USER'], 
        password=os.environ['POSTGRES_PASSWORD'], 
        host=os.environ['POSTGRES_HOST'],
    )
    cursor = connect.cursor()

    logging.info('extract: Create table raw_users_by_posts if not exist.')
    with open('raw_users_by_posts.sql', 'r') as raw_users_by_posts_file:
        script = raw_users_by_posts_file.read()
        cursor.execute(script)

    logging.info('extract: Read posts from API.')
    posts = read_posts(os.environ['URL_API'])

    logging.info('extract: Get maximum post id from raw_users_by_posts.')
    max_id = max_id_in_table(cursor)

    logging.info('extract: Remove posts already exist in raw_users_by_posts.')
    posts = data_filter(posts, max_id)
    
    if len(posts) > 0:
        logging.info('extract: Load posts into raw_users_by_posts.')
        load_into_postgress(posts, cursor)
        connect.commit()

    connect.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    while True:
        time.sleep(int(os.environ['TIMEOUT']))
        
        logging.info('extract: start')
        try:
            main()
        except Exception as exception:
            logging.info(f'extract: failed: {exception}')
        logging.info('extract: end')
