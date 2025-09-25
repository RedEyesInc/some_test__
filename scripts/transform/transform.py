import os
import time
import logging

import psycopg2


if __name__ == "__main__":
    time.sleep(10)
    logging.basicConfig(level=logging.INFO)

    while True:
        time.sleep(45)
        logging.info('transform: Connect to PostgreSQL.')
        connect = psycopg2.connect(
            dbname='posts_db', 
            user='post_user', 
            password='pgpwd4post', 
            host='localhost'
        )
        cursor = connect.cursor()

        logging.info('transform: Create table raw_users_by_posts if not exist.')
        with open('raw_users_by_posts.sql', 'r') as raw_users_by_posts_file:
            script = raw_users_by_posts_file.read()
            cursor.execute(script)

        logging.info('transform: Create table top_users_by_posts if not exist.')
        with open('top_users_by_posts.sql', 'r') as top_users_by_posts_file:
            script = top_users_by_posts_file.read()
            cursor.execute(script)

        logging.info('transform: Update top_users_by_posts.')
        cursor.execute('truncate top_users_by_posts')
        cursor.execute('''
            insert into top_users_by_posts (user_id, posts_cnt, calculated_at)
                select 
                    user_id,
                    count(id) as posts_cnt,
                    NOW() as calculated_at
                from raw_users_by_posts
                group by 
                    user_id
        ''')
        connect.commit()

        connect.close()
