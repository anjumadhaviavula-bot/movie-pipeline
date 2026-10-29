"""Movie data pipeline.

Extracts the MovieLens movies.csv and ratings.csv files plus OMDb API data, cleans/enriches them,
and loads the result into SQLite idempotently.

Usage:
    python etl.py --skip-api     # load CSV data only
    python etl.py --limit 20     # test with 20 API calls
    python etl.py                # full run (resumes where it stopped)
"""
import argparse
import difflib
import logging
import os
import re
import sqlite3
import time
import unicodedata
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
MIN_VOTES = 1000      # a title+year hit with fewer IMDb votes is double-checked via search

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


# ----------------------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------------------
ARTICLES = r"The|A|An|Les|Le|La|L'|Das|Der|Die|El|Il|Los|Las|Un|Une"


def move_article(title):
    """'Usual Suspects, The' -> 'The Usual Suspects'; 'Boot, Das' -> 'Das Boot'."""
    m = re.match(r"^(.*), ({})$".format(ARTICLES), title.strip())
    if not m:
        return title.strip()
    sep = "" if m.group(2).endswith("'") else " "
    return m.group(2) + sep + m.group(1)


def fix_title(title):
    """Main title: drops trailing '(Alt Title)' parts and moves the article to the front."""
    title = title.strip()
    while re.search(r"\S\s*\([^()]*\)$", title):        # keep a title that is only '(...)'
        title = re.sub(r"\s*\([^()]*\)$", "", title).strip()
    return move_article(title)


def title_variants(title):
    """Titles to try on OMDb, best first: main title, alternate titles from
    the trailing '(...)' parts, then spelling variants, then shortened titles.

    'Boot, Das (Boat, The)' -> ['Das Boot', 'The Boat']
    "William Shakespeare's Romeo + Juliet" -> [..., 'Romeo + Juliet']
    'Borat: Cultural Learnings of America ...' -> [..., 'Borat']
    """
    titles = [fix_title(title)]
    rest = title.strip()
    while re.search(r"\S\s*\([^()]*\)$", rest):
        alt = re.search(r"\(([^()]*)\)$", rest).group(1)
        titles.append(move_article(re.sub(r"^a\.k\.a\.\s*", "", alt)))
        rest = rest[:rest.rfind("(")].strip()
    for t in list(titles):
        if " and " in t:
            titles.append(t.replace(" and ", " & "))
        if " & " in t:
            titles.append(t.replace(" & ", " and "))
        if t.startswith("Dr. "):
            titles.append("Doctor " + t[4:])
    main = titles[0]
    m = re.match(r"^[A-Z]\w+ [A-Z]\w+'s (.+)$", main)        # "Bram Stoker's Dracula"
    if m:
        titles.append(m.group(1))
    if ": " in main:                                          # title before the subtitle
        titles.append(main.split(": ")[0])
    return list(dict.fromkeys(t for t in titles if t))   # de-duplicate, keep order


def transform_movies(df):
    """Split year from title, clean title, parse genres, add decade."""
    df = df.drop_duplicates("movieId").copy()
    parts = df["title"].str.extract(r"^(.*)\s\((\d{4})\)\s*$")  # col 0 = title, col 1 = year
    df["clean_title"] = parts[0].fillna(df["title"]).apply(fix_title)
    df["title_variants"] = parts[0].fillna(df["title"]).apply(title_variants)
    df["release_year"] = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    df["decade"] = (df["release_year"] // 10 * 10).astype("Int64")   # feature engineering
    df["genres"] = df["genres"].fillna("").replace("(no genres listed)", "")
    df["genre_list"] = df["genres"].apply(lambda g: [x for x in g.split("|") if x])
    return df


def transform_ratings(df, valid_movie_ids):
    """Drop bad rows and convert Unix timestamps to datetime strings."""
    before = len(df)
    df = (df.dropna().sort_values("timestamp")
            .drop_duplicates(["userId", "movieId"], keep="last").copy())  # newest rating wins
    df = df[df["movieId"].isin(valid_movie_ids)]              # remove orphan ratings
    df = df[df["rating"].between(0.5, 5.0)]
    df["rated_at"] = (pd.to_datetime(df["timestamp"], unit="s")
                      .dt.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Ratings kept: %d of %d", len(df), before)
    return df[["userId", "movieId", "rating", "rated_at"]]


# ----------------------------------------------------------------------
# LOAD (CSV data) - idempotent
# ----------------------------------------------------------------------
def load_movies(conn, df):
    """Upsert movies. Does NOT overwrite OMDb-derived columns (plot, box_office, status)."""
    rows = []
    for r in df.itertuples():
        year = None if pd.isna(r.release_year) else int(r.release_year)
        decade = None if pd.isna(r.decade) else int(r.decade)
        rows.append((int(r.movieId), r.clean_title, year, decade))
    conn.executemany(
        """INSERT INTO movies (movie_id, title, release_year, decade)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(movie_id) DO UPDATE SET
               title = excluded.title,
               release_year = excluded.release_year,
               decade = excluded.decade""",
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
    for attempt in range(retries):
        try:
            resp = session.get(OMDB_URL, params=dict(params, apikey=API_KEY), timeout=10)
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
        if "not found" in err:
            return "not_found", None
        log.warning("OMDb error for %s: %s", params, data.get("Error"))   # e.g. 'Error getting data.'
        return "error", None
    return "error", None


def year_close(data, year, earlier=1, later=3):
    """True if the OMDb 'Year' ('1995' or '2005-2010') is near the MovieLens year.

    MovieLens uses the original release year, OMDb often the later US release
    (Spirited Away: 2001 vs 2003), so the window is wider on the 'later' side.
    A remake with the same title is usually much further away and is rejected.
    """
    m = re.match(r"\d{4}", data.get("Year") or "")
    return year is None or (m is not None and -earlier <= int(m.group()) - year <= later)


def normalize(title):
    """'Amélie' -> 'amelie', 'Harold & Kumar' -> 'harold and kumar' (for comparing titles)."""
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    title = title.lower().replace("&", " and ")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", title).split())


def title_matches(omdb_title, titles):
    """True if OMDb's title is (nearly) one of our title variants.

    Rejects OMDb's fuzzy near-misses such as 'Dr. Dolittle 2' for 'Dr. Dolittle'
    (numbers differ) or 'The Boat Is Full' for 'The Boat' (too different).
    """
    a = normalize(omdb_title)
    for t in titles:
        b = normalize(t)
        if a.replace(" ", "") == b.replace(" ", ""):              # 'Oldboy' vs 'Old Boy'
            return True
        if (re.findall(r"\d+", a) == re.findall(r"\d+", b)
                and difflib.SequenceMatcher(None, a, b).ratio() >= 0.85):
            return True
    return False


def imdb_votes(data):
    """'1,234,567' -> 1234567; 0 if missing."""
    digits = re.sub(r"[^\d]", "", data.get("imdbVotes") or "")
    return int(digits) if digits else 0


def search_candidates(session, query, titles, year):
    """OMDb search results whose title matches and whose year is close.

    Searches with the exact year first, then without it (OMDb may list the
    later US release year). Returns (candidates, had_error).
    """
    for params in ([{"y": year}] if year else []) + [{}]:
        status, data = call_omdb(session, dict(params, s=query, type="movie"))
        if status == "error":
            return [], True
        hits = [h for h in (data or {}).get("Search", [])
                if title_matches(h["Title"], titles) and year_close(h, year)]
        if hits:
            return hits, False
    return [], False


def lookup_movie(session, titles, year):
    """Match a movie on OMDb using only its title(s) and year. Returns (status, data).

    titles comes from title_variants(): main title first, then alternates.

    OMDb's direct title lookup returns *some* entry with that title, not
    necessarily the well-known film (e.g. a short or a making-of with the same
    title and year). So:
    1. title + year lookup: accepted straight away only if the title matches
       and the entry is clearly the real film (>= MIN_VOTES IMDb votes).
    2. title lookup without year (OMDb then returns the most popular entry,
       which may carry the later US release year): same checks, year close.
    3. otherwise OMDb search for each title variant: keep results whose title
       matches and whose year is close, and pick the one with the lowest IMDb
       id (the original film is almost always the oldest IMDb entry).
    4. otherwise fall back to a weak step-1 result (an obscure but exact match).
    Most movies match on the first call; extra calls are spent only on hard cases.
    """
    had_error = False
    weak_match = None

    if year:
        for title in titles:
            status, data = call_omdb(session, {"t": title, "y": year, "type": "movie"})
            had_error = had_error or status == "error"
            if status == "found" and title_matches(data.get("Title", ""), titles):
                if imdb_votes(data) >= MIN_VOTES:
                    return status, data
                weak_match = weak_match or data
                break

    for title in titles:
        status, data = call_omdb(session, {"t": title, "type": "movie"})
        had_error = had_error or status == "error"
        if (status == "found" and title_matches(data.get("Title", ""), titles)
                and year_close(data, year) and imdb_votes(data) >= MIN_VOTES):
            return status, data

    for title in titles:
        hits, err = search_candidates(session, title, titles, year)
        had_error = had_error or err
        if hits:
            best = min(hits, key=lambda h: int(re.sub(r"\D", "", h["imdbID"]) or 0))
            if weak_match and weak_match.get("imdbID") == best["imdbID"]:
                return "found", weak_match
            status, data = call_omdb(session, {"i": best["imdbID"]})
            if status == "found":
                return status, data
            had_error = had_error or status == "error"

    if weak_match:
        return "found", weak_match
    return ("error" if had_error else "not_found"), None


def clean_value(value):
    """OMDb uses the string 'N/A' for missing values -> None."""
    return None if value in (None, "", "N/A") else value


def parse_box_office(value):
    """'$229,947,062' -> 229947062; None if missing/unparseable."""
    value = clean_value(value)
    digits = re.sub(r"[^\d]", "", value) if value else ""
    return int(digits) if digits else None


def enrich_movies(conn, variants, limit=None):
    """Call OMDb for movies not yet processed. Safe to stop and re-run.

    variants maps movie_id -> list of titles to try (from title_variants)."""
    rows = conn.execute(
        """SELECT m.movie_id, m.title, m.release_year
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
        for i, (movie_id, title, year) in enumerate(rows, 1):
            status, data = lookup_movie(session, variants.get(movie_id, [title]), year)
            counts[status] += 1

            if status != "error":                     # re-matched: drop old directors
                conn.execute("DELETE FROM movie_directors WHERE movie_id = ?", (movie_id,))
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
            elif status == "not_found":
                conn.execute(
                    """UPDATE movies SET imdb_id = NULL, plot = NULL, box_office = NULL,
                              omdb_status = 'not_found' WHERE movie_id = ?""", (movie_id,))
            else:
                conn.execute("UPDATE movies SET omdb_status = 'error' WHERE movie_id = ?",
                             (movie_id,))

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
    parser.add_argument("--retry-not-found", action="store_true",
                        help="try OMDb again for movies previously marked not_found")
    args = parser.parse_args()

    if not args.skip_api and not API_KEY:
        raise SystemExit("OMDB_API_KEY missing. Add it to a .env file (see README).")

    conn = init_db()
    movies_raw, ratings_raw = extract_csvs()
    movies = transform_movies(movies_raw)
    ratings = transform_ratings(ratings_raw, set(movies["movieId"]))

    with conn:                                        # one transaction; rolls back on error
        load_movies(conn, movies)
        load_genres(conn, movies)
        load_ratings(conn, ratings)
    log.info("CSV data loaded")

    if not args.skip_api:
        if args.retry_not_found:
            with conn:
                n = conn.execute("UPDATE movies SET omdb_status = 'pending' "
                                 "WHERE omdb_status = 'not_found'").rowcount
            log.info("Retrying %d movies previously not found", n)
        variants = dict(zip(movies["movieId"].tolist(), movies["title_variants"]))
        enrich_movies(conn, variants, limit=args.limit)

    log_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
