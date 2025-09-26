FROM python:latest

WORKDIR /usr/src/app

COPY ./transform.py ./transform.py
COPY ./raw_users_by_posts.sql ./raw_users_by_posts.sql
COPY ./top_users_by_posts.sql ./top_users_by_posts.sql
COPY ./update_top_users_by_posts.sql ./update_top_users_by_posts.sql

RUN pip install --no-cache-dir psycopg2-binary

CMD python3 /usr/src/app/transform.py
