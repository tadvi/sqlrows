# sqlrows

Light helper for DB API specification 2.0 compliant connections making
working with results a lot simpler.

Example:

```python
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
```

```python
import sqlrows
import sqlite3

with sqlite3.connect('users.db') as conn:

    users = sqlrows.Database(conn)
    users.execute("INSERT INTO users VALUES (:name, :pass, :role)",
        {'name': 'bob', 'pass': 'secret', 'role': 1})

```