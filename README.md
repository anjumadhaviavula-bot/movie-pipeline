# Movie Data Pipeline

## Overview

This project is a small ETL pipeline built around two files from the MovieLens "small" dataset, `movies.csv` and `ratings.csv`. For each movie it also pulls extra details from the OMDb API (director, plot and box office), cleans everything up, and loads it into a SQLite database. Once the data is in, four SQL queries answer the analytical questions from the assignment.

```
movies.csv ──┐
ratings.csv ─┼─► Extract ─► Transform ─► Load ─► SQLite (movies.db) ─► queries.sql
OMDb API ────┘   (read CSVs,   (clean, parse,    (upserts, one
                  call API)      enrich)           transaction)
```

A few things I cared about while building it:

- **It can be re-run safely.** Running `etl.py` twice gives you the same database, not duplicate rows.
- **It works within the free API limit.** OMDb's free key allows about 1,000 requests a day, so the pipeline remembers where it stopped and picks up from there the next day.
- **It prefers no answer over a wrong answer.** If a movie can't be matched on OMDb with confidence, it's marked as not found rather than linked to the wrong film.

## Repository structure

```
movie-pipeline/
├── data/                 MovieLens CSVs (movies.csv, ratings.csv)
├── etl.py                the pipeline (extract, transform, load, enrich)
├── schema.sql            CREATE TABLE statements
├── queries.sql           the four analytical queries
├── check_coverage.py     shows how many movies/ratings are enriched so far
├── run_queries.py        runs queries.sql and prints the results
├── requirements.txt
└── README.md
```

## Setup and run

You'll need Python 3.8 or newer and a free OMDb API key from https://www.omdbapi.com/apikey.aspx. One thing that tripped me up: the key only works after you click the activation link OMDb sends by email.

```bash
# 1. Clone the repo
git clone https://github.com/anjumadhaviavula-bot/movie-pipeline.git
cd movie-pipeline

# 2. Create and activate a virtual environment
python -m venv venv
venv\Scripts\activate            # Windows PowerShell
# source venv/bin/activate       # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create a file called .env in the project root with your key
OMDB_API_KEY=your_key_here

# 5. Check that data/ has movies.csv and ratings.csv
#    (they come from MovieLens "ml-latest-small":
#     https://grouplens.org/datasets/movielens/latest/; only these two files are used)

# 6. Run the pipeline
python etl.py --skip-api         # load the CSV data only, no API calls
python etl.py --limit 20         # quick test with 20 movies
python etl.py                    # full run; continues where the last run stopped
python etl.py --retry-not-found  # give movies OMDb didn't match another try
```

`movies.db` is created automatically on the first run. To see the answers to all four questions, run:

```bash
python run_queries.py
```

or open `movies.db` in any SQLite tool (DB Browser for SQLite works well) and run `queries.sql`, or use `sqlite3 movies.db < queries.sql`.

**About the API limit:** there are 9,742 movies and the free key allows roughly 1,000 requests a day, so a full enrichment takes several days. Just run `python etl.py` once a day and it carries on with the movies that are still pending. `python check_coverage.py` shows how far along it is.

## How the pipeline works

**Extract**
- Reads `movies.csv` and `ratings.csv` with pandas. Nothing else from MovieLens is used.
- Looks up each movie on OMDb by its cleaned title and release year. Matching turned out to be the trickiest part of the project, so it has its own section under challenges below.
- If a request fails because of a network problem, it waits and retries (1s, 2s, then 4s). When the daily limit is reached, it saves its progress and stops with a clear message.

**Transform**
- Splits the year out of the title, so `Toy Story (1995)` becomes `Toy Story` and `1995`.
- Puts titles back in their natural order (`Usual Suspects, The` becomes `The Usual Suspects`) so the API can find them.
- Converts the Unix timestamps in the ratings to readable dates, and checks for duplicate ratings, ratings outside 0.5 to 5, and ratings for movies that don't exist. The dataset turned out to be clean, so nothing had to be dropped.
- Cleans up what OMDb sends back: `"N/A"` becomes `NULL`, box office strings like `"$229,947,062"` become numbers, and `"Director A, Director B"` is split into two directors.
- Feature engineering: the pipe-separated genres (`Action|Comedy`) are split into their own table, and a `decade` column is added from the release year.

**Load**
- Every insert is keyed on a primary key (`INSERT ... ON CONFLICT DO UPDATE` or `INSERT OR IGNORE`), so the script can be run as many times as you like without creating duplicates. I checked this by running it twice and comparing the row counts of every table.
- The CSV data is loaded in a single transaction, so if something fails halfway, nothing half-finished is left behind.
- API results are saved every 50 movies, so an interrupted run loses very little work.

## Design choices and assumptions

- **SQLite.** It needs no installation or server, so anyone reviewing this can run it right away. The SQL is standard, so moving to PostgreSQL later would be a small change.
- **Separate tables for genres and directors.** A movie can have several genres and several directors, so storing them as comma-separated text would make the queries messy. With their own tables, "top genres" and "top director" are simple joins.
- **Only the two required files.** The assignment asks for `movies.csv` and `ratings.csv`, so those are the only inputs (plus the OMDb API). The `imdb_id` column is filled in from OMDb's response.
- **No movie is ever dropped.** If OMDb can't find a movie, it stays in the database with `omdb_status = 'not_found'`. Its ratings are still real, so they still count in the queries.
- **Most-rated movies go to the API first.** Since only about 1,000 requests fit in a day, it made sense to start with the movies people actually rated the most. That way even a partial run covers most of the ratings.
- **Q1 needs at least 50 ratings.** Without a minimum, some movie with a single 5-star rating would come out on top, which isn't a meaningful answer. 50 is my judgement call, and it's easy to change in `queries.sql`.
- **Genre averages are taken over individual ratings.** In question 2, a movie with many ratings counts more towards its genre than a movie with only a few. I think that reflects what viewers actually rated, but averaging per movie first would be a valid alternative and would give slightly different numbers.
- **Duplicate ratings keep the latest value.** If the same user rated the same movie twice, the newer rating (by timestamp) wins. This never actually happens in this dataset, but the pipeline handles it anyway.

## Challenges and how I solved them

**1. The API's daily limit.** About 1,000 requests a day against 9,742 movies meant the enrichment could never finish in one go. I added an `omdb_status` column (`pending`, `found`, `not_found`, `error`) so each run knows exactly what's left to do. I also process the most-rated movies first, so a partial result is still useful, and when the limit is hit the script saves its progress and exits cleanly instead of crashing.

**2. Finding the right movie on OMDb (and handling movies that aren't found).** This was the biggest challenge. `movies.csv` has no ID that OMDb understands, so every lookup goes by title, and MovieLens titles often differ from OMDb's: `Usual Suspects, The`, `Boot, Das`, `Twelve Monkeys (a.k.a. 12 Monkeys)`, `Harold and Kumar` vs `Harold & Kumar`.

My first version cleaned the title and asked OMDb for "this title, this year". Most movies were found, but some of the directors that came back made no sense. OMDb returns *a* movie with that title, not necessarily *the* movie: for *Schindler's List* (1993) it returned a little-known entry with the same name instead of Spielberg's film. OMDb also often lists the US release year (*Spirited Away*: 2001 in MovieLens, 2003 on OMDb).

So I made the matching stricter:

- **Try a few versions of the title:** the cleaned title, alternate titles from the brackets, `and`/`&` swaps, and shorter forms (`Romeo + Juliet` for `William Shakespeare's Romeo + Juliet`).
- **Check every answer before accepting it:** the title must match (numbers too, so `Dr. Dolittle 2` is rejected), the year must be close (1 year earlier to 3 years later), and the film needs at least 1,000 IMDb votes, which filters out the lookalikes.
- **If the quick lookup fails,** try the title without a year, then a search, picking the matching entry with the lowest IMDb ID, since the original film is usually the oldest entry.
- **If nothing passes,** the movie is marked `not_found` and kept with its ratings. Temporary API errors are marked `error` and retried on the next run.

In the current run, 2,114 of 2,136 movies were matched (99%). The rest are mostly titles that differ too much, like `Old Boy` vs `Oldboy`. I chose to leave those unmatched rather than loosen the rules, because a wrong director hurts the analysis more than a missing one.

**3. Messy values from the API.** OMDb uses the text `"N/A"` for anything missing, sends box office as a currency string, and puts multiple directors in one comma-separated field. These are turned into `NULL`, plain numbers, and one row per director.

**4. Re-running without duplicates.** A plain `INSERT` (or pandas `to_sql` in append mode) adds the same rows again on every run. Primary keys plus upserts fixed that, and the API step only touches movies that are still pending.

**5. Data types when loading.** SQLite doesn't accept numpy types like `numpy.int64` directly, which caused errors at first. Converting the values to plain Python types before inserting solved it.

**6. Developing against a rate-limited API.** Every test run used up part of the daily quota, and I ran out more than once while working on the matching. To get around this, I tested the title-cleaning and matching rules with small offline checks first, and only then tried them live on small batches with `--limit` before doing a full run. Re-running just the earlier misses with `--retry-not-found` also meant I didn't have to repeat the whole enrichment after each improvement.

**7. Keeping the API key private.** The key lives in a `.env` file that is listed in `.gitignore`, so it never ends up in the repository. While adding error logging, I also noticed that logging the full request would have printed the key into the logs, so the log messages only show the movie being looked up, never the key.

## Results

Status at the time of writing:

| Metric | Value |
|---|---|
| Movies loaded | 9,742 |
| Ratings loaded | 100,836 (none dropped) |
| Genres | 19 |
| Movies sent to OMDb so far | 2,136 (most-rated first) |
| Matched (enriched) | 2,114 (99.0% of those tried; 21.7% of all movies) |
| Not found | 22 (1.0% of those tried) |
| Still pending | 7,606 (picked up on later daily runs) |
| **Share of all ratings covered by enriched movies** | **78.7%** |

That last number is why I enriched the popular movies first: only about a fifth of the movies have been enriched, but they account for almost 80% of all ratings.

**Answers to the questions** (see `queries.sql`):

1. **Highest-rated movie:** *The Shawshank Redemption*, with an average of 4.43 from 317 ratings (minimum 50 ratings).
2. **Top 5 genres by average rating:** Film-Noir (3.92), War (3.81), Documentary (3.80), Crime (3.66) and Drama (3.66). Film-Noir is a small genre (870 ratings, compared with 41,928 for Drama), so its average is less certain.
3. **Director with the most movies:** Steven Spielberg, with 21 movies. This answer depends on the API data, so it only covers the 2,114 movies enriched so far and could change as more are added. Since those are the most popular movies, and the next director (Woody Allen) is at 17, I don't expect it to change.
4. **Average rating per release year:** one row per year, from 1902 onward. The early years have very few ratings (1902 has 5, 1917 has just 1), so their averages are noisy. That's why the query also shows `num_ratings`.

Questions 1, 2 and 4 only use the CSV data, which is fully loaded, so the API coverage doesn't affect them.

## How I checked the results

I didn't want to just trust the output of my own SQL, so I checked it a few different ways:

- **Recomputed the answers separately.** I wrote a quick pandas script that works straight from the raw CSV files, without the database or any of the pipeline code. Its answers for questions 1, 2 and 4 matched the SQL results exactly, including all 106 years in question 4.
- **Checked the edge cases.** The top two genres by rounding (Crime and Drama, both 3.66) are in the right order when you look at the unrounded values (3.658 vs 3.656), and Shawshank's lead in question 1 is clear (4.43 vs 4.29 for *The Godfather*).
- **Spot-checked the API data.** For question 3, I went through the list of movies credited to Steven Spielberg, and all 21 are really his films. Checking directors like this is also how I found the wrong-film matches described in challenge 2.
- **Rebuilt from scratch.** I ran the pipeline in an empty folder containing only the two CSV files, and then ran it a second time to confirm the row counts didn't change.

## What I'd improve with more time

- **Scheduling:** run the pipeline daily with Airflow or Prefect, with retries and alerts, instead of starting it by hand.
- **Database:** move to PostgreSQL or a cloud warehouse (Snowflake) for bigger data and more users.
- **API speed:** a paid OMDb tier to remove the daily cap, parallel requests with rate limiting, and a cache so no movie is ever requested twice.
- **Layered storage:** keep the raw CSVs and API responses (for example in S3) separate from the cleaned data, so transformations can be re-run without calling the API again.
- **Scale:** for the larger MovieLens datasets, process the data in chunks or with Spark, and add indexes or partitioning on the ratings table.
