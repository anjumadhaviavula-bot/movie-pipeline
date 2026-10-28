-- Q1: Movie with the highest average rating
-- Minimum 50 ratings, otherwise a movie with one 5-star vote would win.
SELECT m.title, ROUND(AVG(r.rating), 2) AS avg_rating, COUNT(*) AS num_ratings
FROM ratings r
JOIN movies m ON m.movie_id = r.movie_id
GROUP BY m.movie_id
HAVING COUNT(*) >= 50
ORDER BY avg_rating DESC
LIMIT 1;

-- Q2: Top 5 genres by average rating
SELECT g.name, ROUND(AVG(r.rating), 2) AS avg_rating, COUNT(*) AS num_ratings
FROM ratings r
JOIN movie_genres mg ON mg.movie_id = r.movie_id
JOIN genres g ON g.genre_id = mg.genre_id
GROUP BY g.genre_id
ORDER BY avg_rating DESC
LIMIT 5;

-- Q3: Director with the most movies in the dataset
-- Depends on how many movies have been enriched from OMDb. Ties are possible.
SELECT d.name, COUNT(*) AS movie_count
FROM movie_directors md
JOIN directors d ON d.director_id = md.director_id
GROUP BY d.director_id
ORDER BY movie_count DESC
LIMIT 1;

-- Q4: Average rating of movies released each year
SELECT m.release_year, ROUND(AVG(r.rating), 2) AS avg_rating, COUNT(*) AS num_ratings
FROM ratings r
JOIN movies m ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;