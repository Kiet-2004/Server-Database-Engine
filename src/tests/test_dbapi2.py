from dbapi2 import connect
import time
conn = connect("http://127.0.0.1:8000", "nam", '12345678', "CompanyDB")
cursor = conn.cursor()

queries = [
    # "SELECT * FROM employees WHERE name = 'Alice';",
    # "SELECT id FROM employees WHERE phone LIKE '090%';",
    # "SELECT email FROM employees WHERE email LIKE '%@company.com';",
    # "SELECT name FROM employees WHERE id = 5;",
    # "SELECT phone FROM employees WHERE name = 'Bob';",
    # "SELECT * FROM employees WHERE email IS NOT NULL;",
    # "SELECT name FROM employees WHERE phone IS NULL;",
    # "SELECT * FROM employees WHERE name LIKE 'J%';",
    # "SELECT * FROM employees WHERE email LIKE '%.vn';",
    # "SELECT id FROM employees WHERE name != 'John';",
    "SELECT * FROM tasks WHERE status = 'completed';",
    # "SELECT task FROM tasks WHERE hours > 5.0;",
    # "SELECT id FROM tasks WHERE task LIKE '%report%';",
    # "SELECT * FROM tasks WHERE status = 'pending';",
    # "SELECT hours FROM tasks WHERE task = 'Design UI';",
    # "SELECT * FROM tasks WHERE hours < 2.5;",
    # "SELECT task FROM tasks WHERE status != 'completed';",
    # "SELECT * FROM tasks WHERE task LIKE 'Fix%';",
    # "SELECT id FROM tasks WHERE hours = 8.0;",
    # "SELECT * FROM tasks WHERE status IS NOT NULL;"
]

for query in queries:
    time.sleep(1)
    try:
        cursor.execute(query)
        print(cursor.fetchall())
    except Exception as e:
        print(query)
        print(e)