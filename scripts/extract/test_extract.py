from unittest.mock import MagicMock

from extract import (
    max_id_in_table,
    data_filter,
)


def test_max_id_in_table_empty_table():
    cursor = MagicMock()
    cursor.fetchall = MagicMock(return_value=[(None,)])

    assert max_id_in_table(cursor) == 0


def test_max_id_in_table_non_empty_table():
    cursor = MagicMock()
    cursor.fetchall = MagicMock(return_value=[(10,)])

    assert max_id_in_table(cursor) == 10


def test_data_filter():
    posts = [
        {'id': 1},
        {'id': 2},
        {'id': 3},
        {'id': 4},
        {'id': 5},
        {'id': 6},
    ]
    max_id = 5
    result = [{'id': 6}]

    assert data_filter(posts, max_id) == result
