PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS movies (
    movie_id     INTEGER PRIMARY KEY,          -- MovieLens movieId
    title        TEXT NOT NULL,                -- cleaned title (no year)
    release_year INTEGER,
    decade       INTEGER,                      -- feature engineering: 1990, 2000, ...
    imdb_id      TEXT,
    plot         TEXT,
    box_office   INTEGER,                      -- dollars, NULL if unknown
    omdb_status  TEXT NOT NULL DEFAULT 'pending'   -- pending / found / not_found / error
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name     TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
    genre_id INTEGER NOT NULL REFERENCES genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS directors (
    director_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id    INTEGER NOT NULL REFERENCES movies(movie_id),
    director_id INTEGER NOT NULL REFERENCES directors(director_id),
    PRIMARY KEY (movie_id, director_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id  INTEGER NOT NULL,
    movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
    rating   REAL NOT NULL CHECK (rating BETWEEN 0.5 AND 5.0),
    rated_at TEXT NOT NULL,                    -- 'YYYY-MM-DD HH:MM:SS'
    PRIMARY KEY (user_id, movie_id)
);

CREATE INDEX IF NOT EXISTS idx_ratings_movie ON ratings(movie_id);
CREATE INDEX IF NOT EXISTS idx_movies_year   ON movies(release_year);
