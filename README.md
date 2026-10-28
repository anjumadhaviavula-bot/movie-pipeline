# Movie Data Pipeline

A small ETL pipeline that combines the **MovieLens "small" dataset** (movies and ratings CSVs) with details from the **OMDb API** (director, plot, box office). It cleans and transforms the data and loads it idempotently into a normalized **SQLite** database, then answers analytical questions with SQL.

```
movies.csv ──┐
ratings.csv ─┼─► Extract ─► Transform ─► Load ─► SQLite (movies.db) ─► queries.sql
links.csv ───┤   (read CSVs,   (clean, parse,    (upserts, one
OMDb API ────┘    call API)      enrich)           transaction)
```

## Repository structure

```
movie-pipeline/
├── data/                 MovieLens CSVs (movies.csv, ratings.csv, links.csv)
├── etl.py                the pipeline (extract, transform, load, enrich)
├── schema.sql            CREATE TABLE statements
├── queries.sql           the four analytical queries
├── check_coverage.py     reports how many movies/ratings are enriched
├── run_queries.py        runs queries.sql and prints the results
├── requirements.txt
└── README.md
```

## Setup and run

**Requirements:** Python 3.8+ and a free OMDb API key (https://www.omdbapi.com/apikey.aspx; remember to click the activation link in the email).

```bash
# 1. Clone and enter the project
git clone https://github.com/anjumadhaviavula-bot/movie-pipeline.git
cd movie-pipeline

# 2. Create and activate a virtual environment
python -m venv venv
venv\Scripts\activate            # Windows PowerShell
# source venv/bin/activate       # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your API key (create a file named .env in the project root)
OMDB_API_KEY=your_key_here

# 5. Make sure data/ contains movies.csv, ratings.csv and links.csv
#    (MovieLens "ml-latest-small": https://grouplens.org/datasets/movielens/latest/)

# 6. Run the pipeline
python etl.py --skip-api         # load CSV data only
python etl.py --limit 20         # quick test with 20 API calls
python etl.py                    # full run; resumes where it stopped
```

The database file `movies.db` is created automatically. To run the analytical queries, open it with any SQLite tool (for example DB Browser for SQLite) and run `queries.sql`, or use the `sqlite3` command line:

```bash
sqlite3 movies.db < queries.sql
```

**Note on the OMDb free tier:** the free key allows roughly 1,000 requests per day, and there are 9,742 movies. The pipeline is therefore **resumable**: run `python etl.py` once a day and it continues with the movies that are still pending.

## Data model

Genres and directors are many-to-many relationships (a movie has several genres; a director can direct several movies), so each has its own table plus a junction table. Composite primary keys prevent duplicate rows.

```
movies (movie_id PK, title, release_year, decade, imdb_id, plot, box_office, omdb_status)
   │
   ├── movie_genres (movie_id, genre_id)  ── genres (genre_id PK, name UNIQUE)
   ├── movie_directors (movie_id, director_id) ── directors (director_id PK, name UNIQUE)
   └── ratings (user_id, movie_id) PK, rating, rated_at)
```

| Table | Purpose |
|---|---|
| `movies` | One row per movie: cleaned title, release year, decade, plus OMDb details |
| `genres`, `movie_genres` | Normalized genre list (the `Action\|Comedy` string is split) |
| `directors`, `movie_directors` | Normalized director list (OMDb returns `"A, B"` strings) |
| `ratings` | One row per user rating; primary key `(user_id, movie_id)` |

`movies.omdb_status` records the API outcome (`pending`, `found`, `not_found`, `error`), which is what makes enrichment resumable and auditable.

## How the pipeline works

**Extract**
- Reads `movies.csv`, `ratings.csv` and `links.csv` with pandas.
- Calls the OMDb API for each movie, in this order: by **IMDb ID** (from `links.csv`, an exact match), then by **title + year**, then by **title alone**.
- Retries network failures with exponential backoff (1s, 2s, 4s) and stops cleanly when the daily request limit is reached.

**Transform**
- Splits the release year out of the title (`Toy Story (1995)` becomes `Toy Story` and `1995`).
- Fixes titles with moved articles (`Usual Suspects, The` becomes `The Usual Suspects`) and strips alternate titles in parentheses, so API title lookups succeed.
- Converts Unix timestamps to datetimes, drops duplicate and out-of-range ratings, and removes ratings for unknown movies.
- Converts OMDb's `"N/A"` strings to `NULL`, parses box office (`"$229,947,062"` becomes `229947062`), and splits multi-director strings.
- **Feature engineering:** genres are split into a normalized table, and a `decade` column is derived from the release year.

**Load**
- **Idempotent:** every load uses primary keys with `INSERT ... ON CONFLICT DO UPDATE` or `INSERT OR IGNORE`, so running the script repeatedly never creates duplicates. I verified this by running it twice and comparing row counts for every table.
- The CSV data is loaded in a single transaction, so a failure rolls back cleanly.
- API progress is committed every 50 movies.

## Design choices and assumptions

- **SQLite:** zero setup, so reviewers can run the project immediately. The SQL is standard, and moving to PostgreSQL would need only small changes.
- **Normalized schema:** avoids storing delimited strings in a column and makes genre and director queries simple joins.
- **IMDb ID matching:** `links.csv` (part of the same MovieLens download) maps each movie to its IMDb ID. This gives exact API lookups instead of fuzzy title matching. Title and year are used only as a fallback.
- **Movies are never dropped:** if the API can't find a movie, it stays in the database with `omdb_status = 'not_found'`, because its ratings are still valid.
- **Most-rated movies are enriched first:** because of the API limit, movies are processed in descending order of rating count, so a partial run covers the data that matters most.
- **Q1 uses a minimum of 50 ratings:** otherwise a movie with a single 5-star rating would win. This is an assumption, and the threshold is easy to change in `queries.sql`.
- **Ratings keep the latest value** if a (user, movie) pair appears twice (it doesn't in this dataset).

## Challenges and how I solved them

1. **API daily limit (about 1,000 requests/day vs 9,742 movies).** I made enrichment resumable using the `omdb_status` column, ordered by rating count so partial runs are still valuable, and made the script stop cleanly with a clear message when the limit is hit.
2. **Matching movies between MovieLens and OMDb.** MovieLens titles embed the year, move articles to the end, and sometimes include alternate titles. I added title cleaning, but the bigger win was using IMDb IDs from `links.csv`: in my runs, **all enriched movies were matched with 0 not-found and 0 errors**. The title-based fallback remains for movies with no IMDb link.
3. **Messy API values.** OMDb returns the string `"N/A"` for missing fields, currency strings for box office, and comma-separated directors. These are converted to `NULL`, integers, and separate rows respectively.
4. **Idempotency.** Plain `INSERT` or `to_sql(append)` would duplicate rows on every run. Primary keys plus upserts solved this, and the enrichment step touches only movies still pending.
5. **Type issues when loading.** pandas and numpy types can't be bound directly by SQLite, so values are converted to plain Python types before inserting.

## Results

**Pipeline status (as of this README):**

| Metric | Value |
|---|---|
| Movies loaded | 9,742 |
| Ratings loaded | 100,836 (none dropped) |
| Genres | 19 |
| Movies enriched from OMDb | 998 (10.2%) |
| Not found / errors | 0 / 0 |
| **Share of all ratings covered by enriched movies** | **60.6%** |

**Query answers** (see `queries.sql`):

1. **Highest average rating:** *The Shawshank Redemption*, 4.43 average from 317 ratings (minimum 50 ratings required).
2. **Top 5 genres by average rating:** Film-Noir (3.92), War (3.81), Documentary (3.80), Crime (3.66), Drama (3.66). Smaller genres like Film-Noir have fewer ratings (870) than Drama (41,928), so their averages are less certain.
3. **Director with the most movies:** Steven Spielberg (17 movies). This is **provisional**: it is computed only from the 998 enriched movies and may change as enrichment continues.
4. **Average rating per release year:** one row per year, starting in 1902. Early years have very few ratings (for example 1902 has 5 and 1917 has 1), so those averages are noisy, which is why `num_ratings` is shown.

Queries 1, 2 and 4 use only the CSV data, which is 100% loaded, so they are unaffected by API coverage.

## Possible improvements and scaling

- **Orchestration:** schedule with Airflow or Prefect, with retries, alerting and incremental daily runs instead of manual re-runs.
- **Database:** PostgreSQL or a cloud warehouse (Snowflake, BigQuery, Redshift) for larger data and concurrent users.
- **API throughput:** async or parallel requests with rate limiting, a persistent response cache, or a paid OMDb tier to remove the daily cap.
- **Data quality:** automated checks (for example Great Expectations) and unit tests with pytest for the cleaning functions.
- **Reproducibility:** Docker image, configuration through environment variables, and structured logging and monitoring.
- **Architecture:** store raw API responses and CSVs in object storage (S3) and layer the data (raw, cleaned, analytics-ready), so transformations can be re-run without re-calling the API.
- **Scale:** process the larger MovieLens datasets in chunks, or with Spark, and add indexes or partitioning on the ratings table.
