-- Idempotent: safe to run on a fresh database AND on an existing one.
-- Postgres runs it on first start; the Spark job re-applies it on every
-- start, so schema upgrades never require wiping the data volume.

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

-- Columns added after the first version (upgrade path for old databases).
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS extras_wides   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS extras_noballs INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS extras_byes    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS extras_legbyes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS extras_penalty INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS is_super_over  BOOLEAN NOT NULL DEFAULT FALSE;
-- Every dismissal on the ball: [{"player_out": "..", "kind": "..", "fielders": [".."]}]
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS wickets JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_deliveries_match ON deliveries(match_id);

DROP VIEW IF EXISTS over_statistics;
DROP VIEW IF EXISTS fall_of_wickets;
DROP VIEW IF EXISTS bowling_statistics;
DROP VIEW IF EXISTS batting_statistics;
DROP VIEW IF EXISTS innings_statistics;

-- ---------------------------------------------------------------------------
-- innings_statistics: one row per innings (including super overs)
-- Wickets = dismissals except "retired hurt" (not a wicket in cricket).
-- ---------------------------------------------------------------------------
CREATE VIEW innings_statistics AS
SELECT
    d.match_id,
    d.innings_number,
    BOOL_OR(d.is_super_over)              AS is_super_over,
    MIN(d.batting_team)                   AS batting_team,
    MIN(d.bowling_team)                   AS bowling_team,
    SUM(d.total_runs)                     AS runs,
    SUM(COALESCE(w.wicket_count, 0))      AS wickets,
    COUNT(*) FILTER (
        WHERE d.extras_wides = 0 AND d.extras_noballs = 0
    )                                     AS legal_balls,
    SUM(d.extra_runs)                     AS extras,
    SUM(d.extras_wides)                   AS wides,
    SUM(d.extras_noballs)                 AS noballs,
    SUM(d.extras_byes)                    AS byes,
    SUM(d.extras_legbyes)                 AS legbyes,
    SUM(d.extras_penalty)                 AS penalty
FROM deliveries d
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS wicket_count
    FROM jsonb_array_elements(d.wickets) x
    WHERE x->>'kind' <> 'retired hurt'
) w ON TRUE
GROUP BY d.match_id, d.innings_number;

-- ---------------------------------------------------------------------------
-- batting_statistics: one row per batter per innings, in batting order.
-- Includes a non-striker who was dismissed/stood at the crease without
-- facing a ball (e.g. run out), which a "GROUP BY batter" would miss.
-- ---------------------------------------------------------------------------
CREATE VIEW batting_statistics AS
WITH appearances AS (
    SELECT match_id, innings_number, batter AS player,
           (over_number * 1000 + ball_number) * 2 AS sort_key
    FROM deliveries WHERE batter IS NOT NULL
    UNION ALL
    SELECT match_id, innings_number, non_striker AS player,
           (over_number * 1000 + ball_number) * 2 + 1 AS sort_key
    FROM deliveries WHERE non_striker IS NOT NULL
),
order_seen AS (
    SELECT match_id, innings_number, player, MIN(sort_key) AS sort_key
    FROM appearances
    GROUP BY match_id, innings_number, player
),
scoring AS (
    SELECT
        match_id, innings_number, batter AS player,
        SUM(batter_runs)                            AS runs,
        COUNT(*) FILTER (WHERE extras_wides = 0)    AS balls_faced,
        COUNT(*) FILTER (WHERE batter_runs = 4)     AS fours,
        COUNT(*) FILTER (WHERE batter_runs = 6)     AS sixes
    FROM deliveries
    GROUP BY match_id, innings_number, batter
),
-- Last dismissal of each player in each innings.
dismissals AS (
    SELECT DISTINCT ON (d.match_id, d.innings_number, x->>'player_out')
        d.match_id,
        d.innings_number,
        x->>'player_out' AS player,
        x->>'kind'       AS kind,
        d.bowler,
        (SELECT STRING_AGG(f, ', ')
           FROM jsonb_array_elements_text(COALESCE(x->'fielders', '[]'::jsonb)) f) AS fielders
    FROM deliveries d
    CROSS JOIN LATERAL jsonb_array_elements(d.wickets) x
    ORDER BY d.match_id, d.innings_number, x->>'player_out',
             d.over_number DESC, d.ball_number DESC
)
SELECT
    o.match_id,
    o.innings_number,
    ROW_NUMBER() OVER (
        PARTITION BY o.match_id, o.innings_number ORDER BY o.sort_key, o.player
    )                                              AS batting_position,
    o.player                                       AS batter,
    COALESCE(s.runs, 0)                            AS runs,
    COALESCE(s.balls_faced, 0)                     AS balls_faced,
    COALESCE(s.fours, 0)                           AS fours,
    COALESCE(s.sixes, 0)                           AS sixes,
    ROUND(100.0 * COALESCE(s.runs, 0) / NULLIF(s.balls_faced, 0), 2) AS strike_rate,
    CASE
        WHEN x.kind IS NULL THEN 'not out'
        WHEN x.kind = 'bowled' THEN 'b ' || x.bowler
        WHEN x.kind = 'lbw' THEN 'lbw b ' || x.bowler
        WHEN x.kind = 'caught and bowled' THEN 'c & b ' || x.bowler
        WHEN x.kind = 'caught' AND x.fielders = x.bowler THEN 'c & b ' || x.bowler
        WHEN x.kind = 'caught' THEN 'c ' || COALESCE(x.fielders, 'sub') || ' b ' || x.bowler
        WHEN x.kind = 'stumped' THEN 'st ' || COALESCE(x.fielders, 'sub') || ' b ' || x.bowler
        WHEN x.kind = 'hit wicket' THEN 'hit wicket b ' || x.bowler
        WHEN x.kind = 'run out' THEN
            'run out' || COALESCE(' (' || x.fielders || ')', '')
        ELSE x.kind
    END                                            AS how_out
FROM order_seen o
LEFT JOIN scoring s
       ON s.match_id = o.match_id
      AND s.innings_number = o.innings_number
      AND s.player = o.player
LEFT JOIN dismissals x
       ON x.match_id = o.match_id
      AND x.innings_number = o.innings_number
      AND x.player = o.player;

-- ---------------------------------------------------------------------------
-- bowling_statistics: one row per bowler per innings
-- Runs conceded exclude byes, leg-byes and penalty runs; wides and no-balls
-- are charged to the bowler. Bowler wickets exclude run outs, retirements
-- and obstructing the field.
-- ---------------------------------------------------------------------------
CREATE VIEW bowling_statistics AS
WITH per_over AS (
    SELECT
        match_id, innings_number, bowler, over_number,
        SUM(total_runs - extras_byes - extras_legbyes - extras_penalty) AS runs,
        COUNT(*) FILTER (
            WHERE extras_wides = 0 AND extras_noballs = 0
        ) AS legal_balls
    FROM deliveries
    GROUP BY match_id, innings_number, bowler, over_number
),
bowler_totals AS (
    SELECT
        match_id, innings_number, bowler,
        SUM(runs)         AS runs_conceded,
        SUM(legal_balls)  AS legal_balls,
        -- maiden = a full over (6 legal balls) with no runs charged to the bowler
        COUNT(*) FILTER (WHERE runs = 0 AND legal_balls >= 6) AS maidens
    FROM per_over
    GROUP BY match_id, innings_number, bowler
),
extras_charged AS (
    SELECT
        match_id, innings_number, bowler,
        SUM(extras_wides)   AS wides,
        SUM(extras_noballs) AS noballs
    FROM deliveries
    GROUP BY match_id, innings_number, bowler
),
wickets_taken AS (
    SELECT d.match_id, d.innings_number, d.bowler, COUNT(*) AS wickets
    FROM deliveries d
    CROSS JOIN LATERAL jsonb_array_elements(d.wickets) x
    WHERE x->>'kind' NOT IN (
        'run out', 'retired hurt', 'retired out', 'obstructing the field'
    )
    GROUP BY d.match_id, d.innings_number, d.bowler
)
SELECT
    t.match_id,
    t.innings_number,
    t.bowler,
    t.runs_conceded,
    t.legal_balls,
    t.maidens,
    COALESCE(w.wickets, 0) AS wickets,
    e.wides,
    e.noballs,
    ROUND((t.runs_conceded * 6.0) / NULLIF(t.legal_balls, 0), 2) AS economy
FROM bowler_totals t
JOIN extras_charged e USING (match_id, innings_number, bowler)
LEFT JOIN wickets_taken w USING (match_id, innings_number, bowler);

-- ---------------------------------------------------------------------------
-- fall_of_wickets: team score and over at each wicket (retired hurt excluded)
-- ---------------------------------------------------------------------------
CREATE VIEW fall_of_wickets AS
WITH running AS (
    SELECT
        match_id, innings_number, over_number, ball_number, wickets,
        SUM(total_runs) OVER (
            PARTITION BY match_id, innings_number
            ORDER BY over_number, ball_number
        ) AS score,
        SUM(CASE WHEN extras_wides = 0 AND extras_noballs = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY match_id, innings_number, over_number
            ORDER BY ball_number
        ) AS legal_in_over
    FROM deliveries
)
SELECT
    match_id,
    innings_number,
    ROW_NUMBER() OVER (
        PARTITION BY match_id, innings_number
        ORDER BY over_number, ball_number, w.ord
    ) AS wicket_number,
    w.item->>'player_out' AS player_out,
    w.item->>'kind'       AS kind,
    score,
    (over_number - 1) || '.' || legal_in_over AS over_text
FROM running
CROSS JOIN LATERAL jsonb_array_elements(wickets) WITH ORDINALITY AS w(item, ord)
WHERE w.item->>'kind' <> 'retired hurt';

-- ---------------------------------------------------------------------------
-- over_statistics
-- ---------------------------------------------------------------------------
CREATE VIEW over_statistics AS
SELECT
    d.match_id,
    d.innings_number,
    d.over_number,
    SUM(d.total_runs) AS runs,
    SUM(COALESCE(w.wicket_count, 0)) AS wickets
FROM deliveries d
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS wicket_count
    FROM jsonb_array_elements(d.wickets) x
    WHERE x->>'kind' <> 'retired hurt'
) w ON TRUE
GROUP BY d.match_id, d.innings_number, d.over_number;
