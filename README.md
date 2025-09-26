# some_test__

Запуск задания: ```docker-compose build && docker-compose up```

Спустя пару минут можно проверить итоговую витрину (например в DBeaver) запросом ```select * from top_users_by_posts order by posts_cnt desc limit 10```

Проверка юнит тестов:
```
cd scripts/extract
py -m venv env
./env/Scripts/pip install -r ./requirements.txt
./env/Scripts/pytest -v ./test_extract.py
```
