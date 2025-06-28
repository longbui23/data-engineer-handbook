-- Struct type for film stat
CREATE TYPE film AS (
	film Text,
	votes Integer,
	rating Real,
	filmid Text
);

-- ENUM type for film quality
CREATE TYPE quality_class AS
ENUM('star','good','average','bad');


-- Film Table
CREATE TABLE actors (
	film film[],
	quality_class quality_class,
	is_active BOOLEAN,
	actorId TEXT,
	actor_name TEXT,
	current_year INTEGER
)

SELECT *
FROM actors;


-- Cummulative Table Generation Query
DO $$
DECLARE 
	y INT;
BEGIN
	FOR y IN 1970..2021 LOOP
		INSERT INTO actors
		SELECT
			CASE
				WHEN ls.film IS NULL THEN ARRAY[ROW(ts.film, ts.votes, ts.rating, ts.filmid)::film]
				WHEN ts.year IS NOT NULL THEN ls.film || ARRAY[ROW(ts.film, ts.votes, ts.rating, ts.filmid)::film]
				ELSE ls.film
			END AS film,
			CASE
				WHEN ts.year IS NOT NULL THEN
					(CASE
						WHEN ts.rating > 8.0 THEN 'star'
						WHEN ts.rating > 7 THEN 'good'
						WHEN ts.rating > 6 THEN 'average'
						ELSE 'bad'
					END)::quality_class
				ELSE ls.quality_class
			END AS quality_class,
			ts.year IS NOT NULL AS is_active,
			COALESCE(ls.actorid, ts.actorid) AS actorid,
			COALESCE(ls.actor_name, ts.actor) AS actor_name,
			COALESCE(ts.year, ls.current_year + 1) AS current_year
		FROM 
			(SELECT * FROM actors WHERE current_year = y - 1) ls
			FULL OUTER JOIN
			(SELECT * FROM actor_films WHERE year = y) ts
			ON ls.actorid = ts.actorid;
	END LOOP;
END $$;

-- SCD to track actors_history(active & quality class)
CREATE TABLE actors_scd (
	actorid TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_year integer,
	end_year integer,
	current_year integer
)

-- backfilling data into scd
INSERT INTO actors_scd
WITH quality_changed AS 
(
	SELECT actorid,
		is_active,
		quality_class,
		current_year,
		LAG(quality_class, 1) OVER
			(PARTITION BY actor_id ORDER BY current_year) <> quality_class
		OR LAG(quality_class, 1) OVER
			(PARTITION BY actor_id ORDER BY current_year) IS NULL
		AS did_change
	FROM actors;
), change_identified AS (
	SELECT
		actorid,
		is_active,
		quality_class,
		current_year,
		SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
			OVER (PARTITION BY actorid ORDER BY current_year) AS change_identifier
	FROM quality_changed
), aggregated AS (
	SELECT
		actorid,
		quality_class,
		is_active,
		change_identifier,
		MIN(current_year) AS start_year,
		MAX(current_year) AS end_year,
		'2022' AS current_year
	FROM changed_identified
	GROUP BY 1, 4, 3, 2
	ORDER BY 1,2
)

SELECT actorid, quality_class, is_active, start_year, end_year, current_year
FROM aggregated;


-- Incremental Query for actors_history_scd: combines previous SCD with new incoming data
CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active boolean,
	start_year INTEGER,
	end_year INTEGER
)
						
WITH last_year_scd AS (
	SELECT * FROM actors_scd
	WHERE current_year = 2021
	AND end_date = 2021
), historical_scd AS (
	SELECT
		actorid,
		quality_class,
		is_active,
		start_year,
		end_year
	FROM actors_scd
	WHERE current_year = 2021
	AND end_year < 2021
), this_year_data AS (
	SELECT * FROM actors
	WHERE current_Year = 2022
), unchanged_records AS (
	SELECT
		ts.actorid,
		ts.quality_class,
		ts.is_active,
		ts.start_year,
		ts.current_year AS end_year
	FROM this_year_data ts
	JOIN last_year_scd ls
	ON ls.actorid = ts.actorid
	WHERE ts.quality_class = ls.quality_class
	AND ts.is_active = ls.is_active
), changed_records AS (
	SELECT
		ts.actorid,
		UNNEST(ARRAY[
			ROW(
				ls.quality_class,
				ls.votes,
				ls.start_year,
				ls.end_year
			)::scd_type,
			ROW(
				Ts.quality_class,
				Ts.votes,
				Ts.start_year,
				Ts.end_year
			)::scd_type,
		]) as records
	FROM this_year_data ts
	LEFT JOIN last_year_scd ls
	ON ls.actorid = ts.actorid
	WHERE (ts.quality_class <> ls.quality_class
	OR ts.is_active <> ls.is_active)
), unnested_changed_records AS (
	SELECT actorid,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_year,
		(records::scd_type).end_year
	FROM changed_records
),  new_records AS (
	SELECT
		ts.player_name,
		ts.quality_class,
		ts.is_active,
		ts.current_year as start_year,
		ts.current_year as end_Year
	FROM this_year_data ts
	LEFT JOIN last_season_scd ls
	ON ts.actorid = ls.actorid
	WHERE ls.actorid IS NULL

SELECT *, 2022 AS current_year FROM (
	SELECT *
	FROM historical_scd

	UNION ALL

	SELECT *
	FROM unchanged_records

	UNION ALL

	SELECT *
	FROM unnested_changed_records

	UNION ALL

	SELECT *
	FROM new_records
)


-- Vertices
CREATE TYPE vertex_type
	AS ENUM('actor','film')

CREATE TABLE vertices (
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, type)
);

-- Edges
CREATE TYPE edge_type AS (
	ENUM('acted_in')
);

CREATE TABLE edges (
	subject_identifier TEXT,
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON
	PRIMARY KEY (subject_identifier,
		subject_type,
		object_identifier,
		object_type,
		edge_type)
);

-- Data insertion into vertices
INSERT INTO vertices
SELECT
	filmid AS identifier,
	'film'::vertext_type AS type,
	json_build_object(
		'votes', vote,
		'rating', rating
	) AS properties
FROM actor_films;

INSERT INTO vertices
WITH actors_agg AS (
	SELECT
		actor_id AS identifier,
		MAX(actor) AS actor_name,
		COUNT(1) AS num_films,
		SUM(votes) AS total_votes,
		ARRAY_AGG(DISTINCT filmid) AS films
	FROM actor_films
	GROUP BY 1
)

SELECT identifier,
	'actor'::actor,
	json_build_object(
		'actor_name', actor_name,
		'total_votes', total_votes,
		'films', films
	) AS properties
FROM actors_agg


-- Data insertion into edges
INSERT INTO edges 
SELECT 
	actorid AS subject_identifier,
	'actor'::vertext_type AS subject_type,
	filmid AS object_identifier,
	'film'::vertex_type AS object_type,
	'acted_in'::edge_type AS edge_type,
	json_build_object(
		'votes', votes,
		'rating', rating,
		'year', year,
	) ASA properties
FROM actor_films

-- Query testing
SELECT
	v.properties ->> 'actor_name'
	MAX(CAST(e.properties ->>'votes' AS integer))
FROM vertices v JOIN edges e
ON e.subject_identifier = v.identifier
AND e.subject_type = v.type
WHERE e.properties ->> 'votes' IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC