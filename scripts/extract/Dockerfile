FROM python:latest

WORKDIR /usr/src/app

COPY ./extract.py ./extract.py
COPY ./raw_users_by_posts.sql ./raw_users_by_posts.sql

RUN pip install --no-cache-dir psycopg2-binary requests

CMD python3 /usr/src/app/extract.py
