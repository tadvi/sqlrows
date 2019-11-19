import sqlite3
from contextlib import closing

import pytest
from . import sqlrows

initial_rows = (('A', 1, '101'),
    ('B', 2, '202'),
    ('C', 3, '303'))


@pytest.fixture
def connection():
    connection = sqlite3.connect(':memory:')
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE test (a, b , c)")
    cursor.executemany("INSERT INTO test VALUES (?, ?, ?)", initial_rows)
    cursor.close()
    return connection

def test_iter_rows(connection):
    sql_ = sqlrows.Database(connection)
    for i, row in enumerate(sql_.select("SELECT a, b, c FROM test").iter_rows()):
        assert row == initial_rows[i]

def test_iterator(connection):
    sql_ = sqlrows.Database(connection)
    for i, row in enumerate(sql_.select("SELECT a, b, c FROM test")):
        assert row == dict(zip(('a', 'b', 'c'), initial_rows[i]))

def test_one_column_select_one(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT SUM(b) FROM test").rows == [6]

def test_one_no_result(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT a FROM test WHERE b=4").rows == []

def test_one_multi_no_result(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT a, b FROM test WHERE b=4").rows == []

def test_parameterized_one(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT SUM(b) FROM test WHERE c != ?", ['202']).rows == [4]

def test_make_record_ok(connection):
    with closing(connection.cursor()) as cursor:
        cursor.execute("SELECT a AS aa, c AS cc FROM test")
        fields = sqlrows.Database.make_record(cursor)
    assert 'aa' in fields
    assert 'cc' in fields

def test_make_record_none(connection):
    with closing(connection.cursor()) as cursor:
        cursor.execute("SELECT a AS aa FROM test")
        fields = sqlrows.Database.make_record(cursor)
    assert fields is None

def test_many_column_select_one(connection):
    sql_ = sqlrows.Database(connection)
    record = sql_.select("SELECT a, b FROM test WHERE c = '303'")
    assert record.rows == [('C', 3)]

def test_one_column_select_many(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT b FROM test").rows == [1, 2, 3]

def test_many_column_select_many(connection):
    sql_ = sqlrows.Database(connection)
    records = sql_.select("SELECT b, c FROM test ORDER BY a DESC")
    record = records.rows[0]
    assert (record[0], record[1]) == (3, '303')

def test_all_no_result(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT a FROM test WHERE b=4").rows == []

def test_all_multi_no_result(connection):
    sql_ = sqlrows.Database(connection)
    assert sql_.select("SELECT a, b FROM test WHERE b=4").rows == []


def test_parameterized_all(connection):
    sql_ = sqlrows.Database(connection)
    records = sql_.select("SELECT a, b FROM test WHERE c IN (:1, :2) ORDER BY a",
                       ['101', '202']).rows
    assert records == [('A', 1), ('B', 2)]

def test_run(connection):
    sql_ = sqlrows.Database(connection)
    sql_.execute("DROP TABLE test")
    with pytest.raises(sqlite3.OperationalError):
        sql_.select("SELECT COUNT(*) FROM test")

def test_parameterized_run_one(connection):
    sql_ = sqlrows.Database(connection)
    sql_.execute("INSERT INTO test VALUES (:a, :b, :c)",
             {'a': 'D', 'b': 4, 'c': '404'})
    assert sql_.select("SELECT SUM(b) FROM test").rows == [10]

def test_parameterized_run_many(connection):
    sql_ = sqlrows.Database(connection)
    sql_.execute("INSERT INTO test VALUES (:a, :b, :c)",
             [{'a': 'D', 'b': 4, 'c': '404'}, {'a': 'E', 'b': 5, 'c': '505'}])
    assert sql_.select("SELECT SUM(b) FROM test").rows == [15]

def test_which_execute_case_one_mapping():
    assert sqlrows.Database.which_execute({}) == 'execute'

def test_which_execute_case_one_plain_sequence_list():
    assert sqlrows.Database.which_execute([1, 2, 3]) == 'execute'

def test_which_execute_case_one_plain_sequence_tuple():
    assert sqlrows.Database.which_execute((1, 2, 3)) == 'execute'

def test_which_execute_case_many_sequence_list():
    assert sqlrows.Database.which_execute([[1], [2]]) == 'executemany'

def test_which_execute_case_many_sequence_tuple():
    assert sqlrows.Database.which_execute(([1], [2])) == 'executemany'

def test_which_execute_case_many_sequence_tuple_tuple():
    assert sqlrows.Database.which_execute(((1,), (2,))) == 'executemany'

def test_which_execute_case_many_sequence_tuple_dict():
    assert sqlrows.Database.which_execute(({'a': 1}, {'a': 2})) == 'executemany'

def test_which_execute_empty_list():
    assert sqlrows.Database.which_execute([]) == 'execute'
