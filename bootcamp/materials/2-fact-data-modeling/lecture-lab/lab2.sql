-- Define the "users_cumulated" table
CREATE TABLE users_cumulated (
    user_id TEXT,
	-- dates that online in the past
    date_active DATE[],
	-- today date
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- update cumulated data for a new date
DO $$
DECLARE
    d DATE := DATE '2023-01-01';
    end_date DATE := DATE '2023-01-31';
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO users_cumulated (user_id, date_active, date)
        WITH yesterday AS (
            SELECT *
            FROM users_cumulated
            WHERE date = d - INTERVAL '1 day'
        ),
        today AS (
            SELECT
                CAST(user_id AS TEXT) AS user_id,
                DATE(event_time) AS date_active
            FROM events
            WHERE DATE(event_time) = d
              AND user_id IS NOT NULL
            GROUP BY user_id, DATE(event_time)
        )
        SELECT
            COALESCE(t.user_id, y.user_id) AS user_id,
            CASE
                WHEN y.date_active IS NULL THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.date_active
                ELSE y.date_active || ARRAY[t.date_active]
            END AS date_active,
            d AS date
        FROM today t
        FULL OUTER JOIN yesterday y
            ON t.user_id = y.user_id;

        d := d + INTERVAL '1 day';
    END LOOP;
END $$;

-- Bitops
WITH users AS (
	SELECT * FROM users_cumulated
	WHERE date = DATE('2023-01-31')
), series AS (
	SELECT *
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
), place_holder_int AS (
	SELECT 
		user_id,
		SUM(CASE WHEN
			date_active @> ARRAY[DATE(series_date)]
		THEN POW(2, 32  - (date - DATE(series_date)))
			ELSE 0 END) AS sum_day,
		SUM(CASE WHEN
			date_active @> ARRAY[DATE(series_date)]
		THEN POW(2, 32  - (date - DATE(series_date)))
			ELSE 0 END)::bigint::bit(32) AS datelist_int,
		DATE('2023-03-31') AS date
	FROM users CROSS JOIN series
	GROUP BY 1
)

-- Casting bitops for WAUs, MAUs
SELECT
       user_id,
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