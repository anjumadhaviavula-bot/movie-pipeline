import sqlite3

conn = sqlite3.connect("movies.db")
sql = open("queries.sql", encoding="utf-8").read()
for query in [q.strip() for q in sql.split(";") if q.strip()]:
    cur = conn.execute(query)
    print("\n" + query.splitlines()[0])
    print([d[0] for d in cur.description])
    for row in cur.fetchall():
        print(row)