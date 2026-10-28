import sqlite3

conn = sqlite3.connect("movies.db")

# Share of movies enriched
status = conn.execute(
    "SELECT omdb_status, COUNT(*) FROM movies GROUP BY omdb_status").fetchall()
print("Movies by status:", status)

# Share of all ratings that belong to enriched movies
pct = conn.execute("""
    SELECT ROUND(100.0 * SUM(CASE WHEN m.omdb_status = 'found' THEN 1 ELSE 0 END)
                 / COUNT(*), 1)
    FROM ratings r JOIN movies m ON m.movie_id = r.movie_id
""").fetchone()[0]
print("Ratings covered by enriched movies: {}%".format(pct))

movies_pct = conn.execute("""
    SELECT ROUND(100.0 * SUM(CASE WHEN omdb_status = 'found' THEN 1 ELSE 0 END)
                 / COUNT(*), 1) FROM movies
""").fetchone()[0]
print("Movies enriched: {}%".format(movies_pct))