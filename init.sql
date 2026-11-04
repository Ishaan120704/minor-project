
CREATE TABLE IF NOT EXISTS deliveries (
  event_id TEXT PRIMARY KEY,
  match_id TEXT NOT NULL,
  innings_number INTEGER NOT NULL,
  over_number INTEGER NOT NULL,
  ball_number INTEGER NOT NULL,
  batter TEXT,
  bowler TEXT,
  non_striker TEXT,
  batter_runs INTEGER DEFAULT 0,
  extra_runs INTEGER DEFAULT 0,
  total_runs INTEGER DEFAULT 0,
  extras_type TEXT,
  is_wicket BOOLEAN DEFAULT FALSE,
  wicket_type TEXT,
  player_dismissed TEXT,
  batting_team TEXT,
  bowling_team TEXT,
  event_time TIMESTAMPTZ,
  processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deliveries_match ON deliveries(match_id);

CREATE OR REPLACE VIEW batting_statistics AS
SELECT
    match_id,
    innings_number,
    batter,
    SUM(batter_runs) AS runs,
    COUNT(*) FILTER (
        WHERE extras_type IS DISTINCT FROM 'wides'
    ) AS balls_faced,
    ROUND(
        100.0 * SUM(batter_runs) /
        NULLIF(
            COUNT(*) FILTER (
                WHERE extras_type IS DISTINCT FROM 'wides'
            ), 0
        ), 2
    ) AS strike_rate
FROM deliveries
GROUP BY match_id, innings_number, batter;

CREATE OR REPLACE VIEW bowling_statistics AS
SELECT
    match_id,
    innings_number,
    bowler,
    SUM(
        total_runs -
        CASE
            WHEN extras_type IN ('byes', 'legbyes', 'penalty')
            THEN extra_runs
            ELSE 0
        END
    ) AS runs_conceded,
    COUNT(*) FILTER (
        WHERE extras_type IS DISTINCT FROM 'wides'
          AND extras_type IS DISTINCT FROM 'noballs'
    ) AS legal_balls,
    COUNT(*) FILTER (
        WHERE is_wicket
          AND wicket_type NOT IN (
              'run out',
              'retired hurt',
              'obstructing the field'
          )
    ) AS wickets,
    ROUND(
        (
            SUM(
                total_runs -
                CASE
                    WHEN extras_type IN ('byes', 'legbyes', 'penalty')
                    THEN extra_runs
                    ELSE 0
                END
            ) * 6.0
        ) /
        NULLIF(
            COUNT(*) FILTER (
                WHERE extras_type IS DISTINCT FROM 'wides'
                  AND extras_type IS DISTINCT FROM 'noballs'
            ), 0
        ), 2
    ) AS economy
FROM deliveries
GROUP BY match_id, innings_number, bowler;

CREATE OR REPLACE VIEW over_statistics AS
SELECT
    match_id,
    innings_number,
    over_number,
    SUM(total_runs) AS runs,
    COUNT(*) FILTER (WHERE is_wicket) AS wickets
FROM deliveries
GROUP BY match_id, innings_number, over_number;
