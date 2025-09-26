import os
import time
import logging

import psycopg2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    while True:
        time.sleep(int(os.environ['TIMEOUT']))
        
        logging.info('transform: Connect to PostgreSQL.')
        connect = psycopg2.connect(
            dbname=os.environ['POSTGRES_DB'], 
            user=os.environ['POSTGRES_USER'], 
            password=os.environ['POSTGRES_PASSWORD'], 
            host=os.environ['POSTGRES_HOST'],
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
        with open('update_top_users_by_posts.sql', 'r') as update_top_users_by_posts_file:
            script = update_top_users_by_posts_file.read()
            cursor.execute(script)

        connect.commit()
        connect.close()
