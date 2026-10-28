"""Movie data pipeline.

Extracts MovieLens CSVs and OMDb API data, cleans/enriches them,
and loads the result into SQLite idempotently.

Usage:
    python etl.py --skip-api     # load CSV data only
    python etl.py --limit 20     # test with 20 API calls
    python etl.py                # full run (resumes where it stopped)
"""
import argparse
import logging
import os
import re
import sqlite3
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")
DB_PATH = "movies.db"
DATA_DIR = Path("data")
SCHEMA_PATH = Path("schema.sql")
OMDB_URL = "http://www.omdbapi.com/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class DailyLimitReached(Exception):
    """OMDb free-tier daily request limit hit."""


class InvalidApiKey(Exception):
    """OMDb rejected the API key."""


# ----------------------------------------------------------------------
# DATABASE
# ----------------------------------------------------------------------
def init_db():
    """Create the DB and tables (safe to run repeatedly)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return conn


# ----------------------------------------------------------------------
# EXTRACT
# ----------------------------------------------------------------------
def extract_csvs():
    """Read the raw MovieLens files."""
    movies = pd.read_csv(DATA_DIR / "movies.csv")
    ratings = pd.read_csv(DATA_DIR / "ratings.csv")
    log.info("Read %d movies and %d ratings", len(movies), len(ratings))
    return movies, ratings


def load_imdb_links():
    """Optional: map movieId -> IMDb id ('tt0114709') from links.csv."""
    path = DATA_DIR / "links.csv"
    if not path.exists():
        log.info("links.csv not found; will match OMDb by title + year only")
        return {}
    links = pd.read_csv(path, dtype={"imdbId": str})
    return {int(r.movieId): "tt" + str(r.imdbId).zfill(7)
            for r in links.itertuples() if pd.notna(r.imdbId)}


# ----------------------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------------------
def fix_title(title):
    """'Usual Suspects, The' -> 'The Usual Suspects'; drops a trailing '(Alt Title)'."""
    title = re.sub(r"\s*\([^()]*\)$", "", title).strip()
    m = re.match(r"^(.*), (The|A|An)$", title)
    return "{} {}".format(m.group(2), m.group(1)) if m else title


def transform_movies(df):
    """Split year from title, clean title, parse genres, add decade."""
    df = df.drop_duplicates("movieId").copy()
    parts = df["title"].str.extract(r"^(.*)\s\((\d{4})\)\s*$")  # col 0 = title, col 1 = year
    df["clean_title"] = parts[0].fillna(df["title"]).apply(fix_title)
    df["release_year"] = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    df["decade"] = (df["release_year"] // 10 * 10).astype("Int64")   # feature engineering
    df["genres"] = df["genres"].fillna("").replace("(no genres listed)", "")
    df["genre_list"] = df["genres"].apply(lambda g: [x for x in g.split("|") if x])
    return df


def transform_ratings(df, valid_movie_ids):
    """Drop bad rows and convert Unix timestamps to datetime strings."""
    before = len(df)
    df = df.dropna().drop_duplicates(["userId", "movieId"]).copy()
    df = df[df["movieId"].isin(valid_movie_ids)]              # remove orphan ratings
    df = df[df["rating"].between(0.5, 5.0)]
    df["rated_at"] = (pd.to_datetime(df["timestamp"], unit="s")
                      .dt.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Ratings kept: %d of %d", len(df), before)
    return df[["userId", "movieId", "rating", "rated_at"]]


# ----------------------------------------------------------------------
# LOAD (CSV data) - idempotent
# ----------------------------------------------------------------------
def load_movies(conn, df, imdb_map):
    """Upsert movies. Does NOT overwrite OMDb-derived columns (plot, box_office, status)."""
    rows = []
    for r in df.itertuples():
        year = None if pd.isna(r.release_year) else int(r.release_year)
        decade = None if pd.isna(r.decade) else int(r.decade)
        rows.append((int(r.movieId), r.clean_title, year, decade, imdb_map.get(int(r.movieId))))
    conn.executemany(
        """INSERT INTO movies (movie_id, title, release_year, decade, imdb_id)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(movie_id) DO UPDATE SET
               title = excluded.title,
               release_year = excluded.release_year,
               decade = excluded.decade,
               imdb_id = COALESCE(movies.imdb_id, excluded.imdb_id)""",
        rows)


def load_genres(conn, df):
    """Fill genres + movie_genres (INSERT OR IGNORE keeps it idempotent)."""
    for r in df.itertuples():
        for g in r.genre_list:
            conn.execute("INSERT OR IGNORE INTO genres (name) VALUES (?)", (g,))
            conn.execute(
                """INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                   SELECT ?, genre_id FROM genres WHERE name = ?""",
                (int(r.movieId), g))


def load_ratings(conn, df):
    """Upsert ratings in bulk (tolist() converts numpy types to plain Python)."""
    rows = list(zip(df["userId"].tolist(), df["movieId"].tolist(),
                    df["rating"].tolist(), df["rated_at"].tolist()))
    conn.executemany(
        """INSERT INTO ratings (user_id, movie_id, rating, rated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(user_id, movie_id) DO UPDATE SET
               rating = excluded.rating, rated_at = excluded.rated_at""",
        rows)


# ----------------------------------------------------------------------
# OMDb API: extract + clean + load, resumable
# ----------------------------------------------------------------------
def call_omdb(session, params, retries=3):
    """One OMDb request. Returns (status, data); status is found / not_found / error."""
    params = dict(params, apikey=API_KEY)
    for attempt in range(retries):
        try:
            resp = session.get(OMDB_URL, params=params, timeout=10)
            data = resp.json()
        except (requests.RequestException, ValueError):
            time.sleep(2 ** attempt)                  # backoff: 1s, 2s, 4s
            continue
        if data.get("Response") == "True":
            return "found", data
        err = data.get("Error", "").lower()
        if "limit" in err:
            raise DailyLimitReached()
        if "api key" in err:
            raise InvalidApiKey()
        return "not_found", None
    return "error", None


def lookup_movie(session, title, year, imdb_id):
    """Try IMDb id, then title+year, then title alone. Returns (status, data)."""
    attempts = []
    if imdb_id:
        attempts.append({"i": imdb_id})
    if year:
        attempts.append({"t": title, "y": year, "type": "movie"})
    attempts.append({"t": title, "type": "movie"})

    had_error = False
    for params in attempts:
        status, data = call_omdb(session, params)
        if status == "found":
            return status, data
        had_error = had_error or status == "error"
    return ("error" if had_error else "not_found"), None


def clean_value(value):
    """OMDb uses the string 'N/A' for missing values -> None."""
    return None if value in (None, "", "N/A") else value


def parse_box_office(value):
    """'$229,947,062' -> 229947062; None if missing/unparseable."""
    value = clean_value(value)
    digits = re.sub(r"[^\d]", "", value) if value else ""
    return int(digits) if digits else None


def enrich_movies(conn, limit=None):
    """Call OMDb for movies not yet processed. Safe to stop and re-run."""
    rows = conn.execute(
        """SELECT m.movie_id, m.title, m.release_year, m.imdb_id
           FROM movies m
           LEFT JOIN (SELECT movie_id, COUNT(*) AS n
                      FROM ratings GROUP BY movie_id) r ON r.movie_id = m.movie_id
           WHERE m.omdb_status IN ('pending', 'error')
           ORDER BY COALESCE(r.n, 0) DESC, m.movie_id""").fetchall()
    if limit:
        rows = rows[:limit]
    log.info("Enriching %d movies from OMDb", len(rows))

    session = requests.Session()
    counts = {"found": 0, "not_found": 0, "error": 0}
    try:
        for i, (movie_id, title, year, imdb_id) in enumerate(rows, 1):
            status, data = lookup_movie(session, title, year, imdb_id)
            counts[status] += 1

            if status == "found":
                conn.execute(
                    """UPDATE movies SET imdb_id = ?, plot = ?, box_office = ?,
                              omdb_status = 'found' WHERE movie_id = ?""",
                    (clean_value(data.get("imdbID")), clean_value(data.get("Plot")),
                     parse_box_office(data.get("BoxOffice")), movie_id))
                for name in (clean_value(data.get("Director")) or "").split(","):
                    name = name.strip()
                    if name:
                        conn.execute("INSERT OR IGNORE INTO directors (name) VALUES (?)", (name,))
                        conn.execute(
                            """INSERT OR IGNORE INTO movie_directors (movie_id, director_id)
                               SELECT ?, director_id FROM directors WHERE name = ?""",
                            (movie_id, name))
            else:
                conn.execute("UPDATE movies SET omdb_status = ? WHERE movie_id = ?",
                             (status, movie_id))

            if i % 50 == 0:
                conn.commit()                         # save progress regularly
                log.info("Progress %d/%d %s", i, len(rows), counts)
            time.sleep(0.1)                           # be polite to the API
    except DailyLimitReached:
        log.warning("Daily OMDb limit reached. Re-run tomorrow to continue.")
    except InvalidApiKey:
        log.error("OMDb rejected the API key. Check OMDB_API_KEY in .env")
    finally:
        conn.commit()
    log.info("Enrichment finished: %s", counts)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def log_summary(conn):
    """Log row counts per table and OMDb match statistics."""
    for table in ("movies", "genres", "movie_genres", "directors", "movie_directors", "ratings"):
        n = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        log.info("%-16s %d rows", table, n)
    for status, n in conn.execute("SELECT omdb_status, COUNT(*) FROM movies GROUP BY omdb_status"):
        log.info("omdb_status %-10s %d", status, n)


def main():
    parser = argparse.ArgumentParser(description="MovieLens + OMDb ETL pipeline")
    parser.add_argument("--limit", type=int, help="max movies to call the API for this run")
    parser.add_argument("--skip-api", action="store_true", help="load CSV data only")
    args = parser.parse_args()

    if not args.skip_api and not API_KEY:
        raise SystemExit("OMDB_API_KEY missing. Add it to a .env file (see README).")

    conn = init_db()
    movies_raw, ratings_raw = extract_csvs()
    movies = transform_movies(movies_raw)
    ratings = transform_ratings(ratings_raw, set(movies["movieId"]))

    with conn:                                        # one transaction; rolls back on error
        load_movies(conn, movies, load_imdb_links())
        load_genres(conn, movies)
        load_ratings(conn, ratings)
    log.info("CSV data loaded")

    if not args.skip_api:
        enrich_movies(conn, limit=args.limit)

    log_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
