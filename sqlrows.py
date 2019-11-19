"""
This is reworked and adopted from "DB API 2.0 for Humans".
Credits: Eugene Van den Bulke for http://github.com/3kwa/sql

Light helper for DB API specification 2.0 compliant connections making
working with results a lot simpler.

Example:

import sqlrows
import sqlite3 # or import mysql.connector

with sqlite3.connect('users.db') as conn:
    # or
    # conn = mysql.connector.connect(user='root', password='mysql',
    #           host='dbserver', port='3306', database='users')

    users = sqlrows.Database(conn)

    rec = users.select('select * from users')
    print(rec.fields)
    print(rec.rows)

    for row in rec.iter_rows():
        print(row)

"""

from contextlib import closing

class RecordSet():
    """
    Holds field names, rows and provides iterator.
    """
    def __init__(self, fields, rows):
        self.fields = fields
        self.rows = rows

    def __iter__(self):
        """ Returns row as dict iterator. """
        self.index = -1
        return self

    def __next__(self):
        """ Returns dict with field:value during iteration. """
        self.index += 1
        if self.index >= len(self.rows):
            raise StopIteration()
        return dict(zip(self.fields, self.rows[self.index]))

    def iter_rows(self):
        """ Returns rows iterator """
        self.index = 0

        while self.index < len(self.rows):
            yield self.rows[self.index]
            self.index += 1

    def __len__(self):
        return len(self.rows)


class Database():
    """
    Instantiate with a DB API v 2.0 connection then use one, all or run method.
    """
    def __init__(self, connection):
        self.connection = connection

    def select(self, query, parameters=None):
        """
        fetchall returning list of scalars or namedtuples
        """
        with closing(self.connection.cursor()) as cursor:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)

            fields = self.make_record(cursor)
            if fields is None:
                return RecordSet(fields, [record[0] for record in cursor.fetchall()])
            return RecordSet(fields, cursor.fetchall())

    def execute(self, query, parameters=None):
        """
        execute or executemany depending on parameters
        """
        with closing(self.connection.cursor()) as cursor:
            execute = getattr(cursor, self.which_execute(parameters))
            if parameters:
                execute(query, parameters)
            else:
                execute(query)

    @staticmethod
    def make_record(cursor):
        """
        return dict suitable for fetching result from cursor
        """
        if len(cursor.description) > 1:
            return {description[0]:i for i, description in enumerate(cursor.description)}
        return None

    @staticmethod
    def which_execute(parameters_or_seq):
        """
        which of execute or executemany
        """
        if not parameters_or_seq:
            return 'execute'

        sequences = (list, tuple)
        types_many = (dict, list, tuple)
        if isinstance(parameters_or_seq, dict):
            return 'execute'

        if any(isinstance(parameters_or_seq, type_) for type_ in sequences):
            if any(isinstance(parameters_or_seq[0], type_) for type_ in types_many):
                return 'executemany'
            return 'execute'
        return 'execute'
