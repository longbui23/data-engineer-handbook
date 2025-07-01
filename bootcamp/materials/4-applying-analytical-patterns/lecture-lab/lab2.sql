CREATE TABLE web_events_dashboard AS 
WITH combined AS (
	SELECT 
		COALESCE(d.browser_type, 'N/A') AS browser_type,
		COALESCE(d.os_type, 'N/A') AS os_type,	
		we.*,
		--  mapping referrer to value
		CASE
		   WHEN referrer like '%linkedin%' THEN 'Linkedin'
		   WHEN referrer like '%t.co%' THEN 'Twitter'
		   WHEN referrer like '%google%' THEN 'Google'
		   WHEN referrer like '%lnkd%' THEN 'Linkedin'
		   WHEN referrer like '%eczachly%' THEN 'On Site'
		   WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
		   ELSE 'Other'
		END as referrer_mapped,
	FROM bootcamp.web_events we
	JOIN bootcamp.devices d
	ON we.device_id = d.device_id
	WHERE url LIKE '%user%'
)

SELECT COALESCE(referrer_mapped, '(overall)') as referrer,
	COALESCE(browser_type, '(overall)') as browser_type,
	COALESCE(os_type, '(overall)') as os_type,
	COUNT(1) AS number_of_site_hits,
	COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
	COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
	CAST(COUNT(CASE WHEN url=/signup THEN 1 END) AS REAL/COUNT(1) AS pct_visited_signup
FROM combined
GROUP BY GROUPING SETS(
	(referrer_mapped, browser_type, os_type),
	(os_type),
	(browser_type),
	(referrer_mapped),
	()
)
-- GROUP BY ROLLUP(referrer_mapped, browser_type, os_type)
HAVING COUNT(1) > 100
ORDER BY CAST(COUNT(CASE WHEN url=/signup THEN 1 END) AS REAL)/COUNT(1) DESC


-- url & event_time --> user flow aggregated analytics
WITH aggregates AS (
	SELECT c1.user_id , 
		c1.url as to_url, 
		c2,url as from_url,
		MIN(c1.event_time - c2.event_time) AS duration
	FROM combined c1 JOIN combined c2
	ON c1.user_id = c2.user_id
	AND DATE(c1.event_time) = DATE(c2.event_time)
	AND c1.event_time > c2.event_time
	GROUP BY 1,2,3
)

SELECT to_url, from_url
	COUNT(1) AS number_of_users,
	MIN(duration) AS min_duration,
	MAX(duration) AS max_duration,
	AVG(duration) AS avg_duration
FROM aggregated
GROUP BY 1,2
HAVING COUNT(1) > 1000
LIMIT 100