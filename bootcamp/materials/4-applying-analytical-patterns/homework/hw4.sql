-- player historical track
CREATE TABLE players_history (
	player_name TEXT,
	first_active_season INTEGER,
	last_active_season INTEGER,
	current_status TEXT
	season_active INTEGER[],
	current_season INTEGER,
	PRIMARY KEY (player, current_season)
)

-- data insertion
INSERT INTO players_history
WITH yesterday AS (
	SELECT *
	FROM player_seasons
	WHERE season = 1995
), today AS (
	SELECT
		player_name,
		season
	FROM player_seasons
	WHERE season = 1996
)

SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(y.first_active_season, t.season) AS first_active_season,
	COALESCE(t.season, y.last_active_season) AS last_active_season,
	-- Season status --
	CASE
		WHEN y.player_name IS NULL THEN 'New'
		WHEN 
			t.player_name IS NOT NULL AND 
			y.last_active_season < t.season - 1 THEN 'Returned From Retirement'
		WHEN 
			t.player_name IS NULL AND
			y.last_active_season < t.season - 1 THEN 'Retired' 
		WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
		WHEN y.current_status IS 'Retired' THEN 'Stay Retired'
		ELSE 'Unknown'
		END AS current_status,
		-- All active Seasons
		COALESCE(y.season_active, ARRAY []::INT[])|| 
			CASE WHEN t.player_name IS NOT NULL THEN ARRAY [t.season]
			ELSE ARRAY []::DATE[]
		END  AS season_active,
		COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y



ON t.player_name = y.player_name;


-- aggegate player and team, player and season, team
SELECT
  player_name,
  team_name,
  season,
  SUM(points) AS total_points,
  COUNT(DISTINCT CASE WHEN win = 'W' THEN game_id END) AS games_won
FROM game_details
GROUP BY GROUPING SETS (
  (player_name, team_name),    
  (player_name, season),         
  (team_name)                   
)
ORDER BY total_points DESC NULLS LAST;


WITH team_results AS (
  SELECT
    team_name,
    game_date,
    win,
    ROW_NUMBER() OVER (PARTITION BY team_name ORDER BY game_date) AS rn
  FROM game_details
),
rolling_win_counts AS (
  SELECT
    a.team_name,
    a.game_date,
    COUNT(*) FILTER (WHERE b.win = 'W') AS wins_in_90_games
  FROM team_results a
  JOIN team_results b
    ON a.team_name = b.team_name
    AND b.rn BETWEEN a.rn AND a.rn + 89
  GROUP BY a.team_name, a.game_date
)
SELECT *
FROM rolling_win_counts
ORDER BY wins_in_90_games DESC
LIMIT 1;
