-- DDL for user_devices_cumulated
CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	browser_type TEXT,
	device_activity_datelist  DATE[],
	date DATE,
	PRIMARY KEY (user_id, date)
)

-- dedupplicated game_details table
WITH deduplicated AS (
    SELECT DISTINCT ON (game_id, player_id) *
    FROM game_details
)

-- Cummulative Query
DO $$
DECLARE
	d DATE := DATE '2023-01-01';
	end_date DATE := DATE '2023-01-31';
BEGIN
	WHILE d <= end_date LOOP
		INSERT INTO user_devices_cumulated
		WITH user_device AS (
			SELECT user_id, 
				browser_type, 
				DATE(event_time) as event_time
			FROM events
			JOIN devices USING(device_id)
			WHERE user_id IS NOT NULL
		),today AS (
			SELECT
				CAST(user_id AS TEXT) as user_id,
				browser_type,
				DATE(event_time) AS date_active
			FROM user_device
			WHERE DATE(event_time) = d
		), yesterday AS (
			SELECT *
			FROM user_devices_cumulated
			WHERE DATE(date) = d - INTERVAL '1 day'
		)
		
		SELECT
			COALESCE(t.user_id, y.user_id) AS user_id,
			COALESCE(t.browser_type, y.browser_type) AS browser,
			CASE
				WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
				WHEN t.date_active IS NULL THEN y.device_activity_datelist
				ELSE y.device_activity_datelist || ARRAY[t.date_active]
			END AS device_activity_datelist,
			t.date_active as date
		FROM today t
		FULL OUTER JOIN yesterday y 
			ON t.user_id = y.user_id
			AND t.browser_type = y.browser_type
	
		d := d + INTERVAL '1 day'
	END LOOP;
END $$;

-- datelist_int column conversion
WITH user_devices AS (
	SELECT * FROM user_devices_cumulated,
	WHERE date = DATE('2023-01-31')
), series AS(
	SELECT *
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
), place_holder_int AS (
	SELECT 
		user_id,
		browser_type,
		SUM(CASE WHEN
			date_active @> ARRAY[DATE(series_date)]
		THEN POW(2, 32  - (date - DATE(series_date)))
			ELSE 0 END) AS sum_day,
		SUM(CASE WHEN
			date_active @> ARRAY[DATE(series_date)]
		THEN POW(2, 32  - (date - DATE(series_date)))
			ELSE 0 END)::bigint::bit(32) AS datelist_int,
		DATE('2023-03-31') AS date
	FROM user_devices CROSS JOIN series
	GROUP BY 1,2
)
		
SELECT
	user_id,
	devices_id,
	datelist_int,
	BIT_COUNT(datelist_int) > 0 AS monthly_active,
       BIT_COUNT(datelist_int) AS l32,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32))) > 0 AS weekly_active,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32)))  AS l7,

       BIT_COUNT(datelist_int &
       CAST('00000001111111000000000000000000' AS BIT(32))) > 0 AS weekly_active_previous_week
FROM place_holder_int;

-- DDL for hosts_cumulated 
CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(host, date)
)

-- Incremental Query for hosts_cumulated
DO $$
DECLARE
    d DATE := DATE '2023-01-01';
    end_date DATE := DATE '2023-01-31';
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO hosts_cumulated
        WITH yesterday AS (
            SELECT *
            FROM hosts_cumulated
            WHERE date = d - INTERVAL '1 day'
        ),
        today AS (
            SELECT
                host
                DATE(event_time) AS date_active
            FROM events
            WHERE DATE(event_time) = d
              AND host IS NOT NULL
        )
        SELECT
            COALESCE(t.host, y.host) AS host,
            CASE
                WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.host_activity_datelist
                ELSE y.host_activity_datelist || ARRAY[t.date_active]
            END AS date_active,
            d AS date
        FROM today t
        FULL OUTER JOIN yesterday y
            ON t.host = y.host;

        d := d + INTERVAL '1 day';
    END LOOP;
END $$;


-- DDL for host_activity_reduced
CREATE TABLE host_activity_reduced (
	host TEXT,
	month_start DATE,
	hit_arr REAL[],
	unique_visitors REAL[],
	PRIMARY KEY (user_id, month_start, )
)

-- Incremental Query
WITH daily_aggregate AS (
	SELECT 
		host,
		DATE(event_date) AS date,
		COUNT(1) AS num_hits,
		COUNT(DISTINCT user_id) AS distinct_visitors
	FROM events
	WHERE DATE(event_time) = DATE('2023-01-01')
	AND user_id IS NOT NULL
), yesterday_array AS (
	SELECT *
	FROM host_activity_reduced
	WHERE month_start = DATE('2023-01-01')
)

SELECT
	COALESCE(da.host, ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
	-- update hit arr
	CASE
		WHEN ya.hit_arr IS NOT NULL THEN
			ya.hit_arr || ARRAY[COALESCE(da.num_hits, 0)]
		WHEN ya.hit_arr IS NULL THEN
			 ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_site_hits,0)]
		END AS hit_arr,
	-- update unique visitors
	CASE
		WHEN ya.unique_visitors IS NOT NULL THEN
			ya.unique_visitors || ARRAY[COALESCE(da.distinct_visitors, 0)]
		WHEN ya.unique_visitors IS NULL THEN
			 ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.distinct_visitors,0)]
		END AS unique_visitors
		
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
ON da.host = ya.host
ON CONFLICT (host, month_start, hit_arr)
DO
	UPDATE SET host_activity_reduced = EXCLUDED.host_activity_reduced