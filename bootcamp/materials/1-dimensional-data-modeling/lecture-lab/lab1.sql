 -- Create Types
 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');


-- Create Table
 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     season_stats season_stats[],
     scoring_class scoring_class,
     years_since_last_season INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );

-- incremental pipeline (loop from 1995 to 2022)
DO $$
DECLARE
  y INT;
BEGIN
  FOR y IN 1995..2022 LOOP
    INSERT INTO players
    SELECT
      COALESCE(ls.player_name, ts.player_name) as player_name,
      COALESCE(ls.height, ts.height) as height,
      COALESCE(ls.college, ts.college) as college,
      COALESCE(ls.country, ts.country) as country,
      COALESCE(ls.draft_year, ts.draft_year) as draft_year,
      COALESCE(ls.draft_round, ts.draft_round) as draft_round,
      COALESCE(ls.draft_number, ts.draft_number) as draft_number,
      CASE 
        WHEN ls.seasons_stats IS NULL THEN ARRAY[ROW(ts.season, ts.gp, ts.pts, ts.reb, ts.ast)::season_stats]
        WHEN ts.season IS NOT NULL THEN ls.seasons_stats || ARRAY[ROW(ts.season, ts.gp, ts.pts, ts.reb, ts.ast)::season_stats] -- retired
        ELSE ls.seasons_stats -- not retired
      END AS seasons_stats,
      CASE 
        WHEN ts.season IS NOT NULL THEN
          (CASE 
             WHEN ts.pts > 20 THEN 'star'
             WHEN ts.pts > 15 THEN 'good'
             WHEN ts.pts > 10 THEN 'average'
             ELSE 'bad' 
           END)::scoring_class
        ELSE ls.scoring_class
      END as scoring_class,
      -- cannot be null since every player has inital starting season when inserting in the table
      CASE 
        WHEN ts.season IS NOT NULL THEN 0 
        ELSE ls.years_since_last_season + 1 
      END AS years_since_last_season,
	   ts.season IS NOT NULL as is_active,
      COALESCE(ts.season, ls.current_season + 1) AS current_season
    FROM
      (SELECT * FROM players WHERE current_season = y - 1) ls
      FULL OUTER JOIN
      (SELECT * FROM player_seasons WHERE season = y) ts
      ON ts.player_name = ls.player_name;
  END LOOP;
END $$;

-- analytical query
  SELECT player_name,
         UNNEST(seasons)
  FROM players
  WHERE current_season = 1998
  AND player_name = 'Michael Jordan';

 SELECT player_name,
        (season_stats[cardinality(season_stats)]::season_stats).pts/
         CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
             ELSE  (season_stats[1]::season_stats).pts END
            AS ratio_most_recent_to_first
 FROM players
 WHERE current_season = 2001
 ORDER BY 2 DESC;