DROP TABLE users_growth_accounting;
CREATE TABLE users_growth_accounting (
	user_id NUMERIC,
	first_active_date DATE,
	last_active_date DATE,
	daily_active_state TEXT,
	weekly_active_state TEXT,
	dates_active DATE[],
	date DATE,
	PRIMARY KEY(user_id, date)
);

-- data insertion
INSERT INTO users_growth_accounting
WITH yesterday AS (
	SELECT * FROM users_growth_accounting
	WHERE date = DATE('2023-02-28')
), today AS (
	SELECT
		user_id,
		DATE_TRUNC('day',event_time::timestamp) AS today_date,
		COUNT(1)
	FROM events
	WHERE DATE_TRUNC('day',event_time::timestamp) = DATE('2023-01-03')
	AND user_id IS NOT NULL
	GROUP BY 1,2
)

SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(y.first_active_date, t.today_date) AS first_active_date,
	COALESCE(t.today_date, y.last_active_date) AS last_active_date,
	-- Daily active state
	CASE 
		WHEN y.user_id IS NULL THEN 'New'
		WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Retained'
		WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
		ELSE 'Stale'
	END AS daily_active_state,
	-- Weekly_active state
	CASE 
		WHEN y.user_id IS NULL THEN 'New'
		WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
		WHEN 
			t.today_date IS NULL 
			AND y.last_active_date  = y.date - Interval '7 day' THEN 'Churned'
		WHEN COALESCE(t.today_date, y.last_active_date) + Interval '7 day' >= y.date THEN 'Retained'
		ELSE 'Stale'
	END AS weekly_active_state,
	-- List of online dates
	COALESCE(y.dates_active, ARRAY []::DATE[])|| 
		CASE WHEN t.user_id IS NOT NULL THEN ARRAY [t.today_date]
		ELSE ARRAY []::DATE[]
	END  AS date_list,
	COALESCE(t.today_date, y.date + Interval '1 day') AS date
	
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id

-- Funnel Analtyics
SELECT
       date - first_active_date AS days_since_first_active,
       CAST(COUNT(CASE
           WHEN daily_active_state
                    IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1) FROM users_growth_accounting
GROUP BY date - first_active_date;