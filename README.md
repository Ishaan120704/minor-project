# Simple Real-Time Cricket Analytics Project

This college-demo project replays a Cricsheet JSON match through Python, Kafka, PySpark Structured Streaming, PostgreSQL and Streamlit.

## Requirements
- Docker Desktop
- Python 3.10+ (only for the replay producer)
- Internet for the first Docker build

## Setup
1. Put one or more Cricsheet T20 JSON matches in `data/` (e.g. `data/match.json`, `data/match2.json`). The file name (without `.json`) becomes the match id.
2. Start the pipeline (Kafka, topic creation, PostgreSQL, Spark, dashboard):
   ```bash
   docker compose up -d --build
   docker compose logs -f spark
   ```
3. In another terminal, install the producer dependency and replay a match:
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   python producer/replay_cricsheet.py --file data/match.json --delay 2
   ```
   The producer talks to Kafka on `localhost:9094` (change with `--bootstrap`).
4. Open http://localhost:8501 and pick a match from the dropdown.

You can replay the same match again, or a different one, at any time; no clean-up is needed (see below).

## Stop / reset
- Stop: `docker compose down`
- Stop and delete all stored data: `docker compose down -v`

> `database/init.sql` is idempotent and Spark re-applies it every time it starts, so schema upgrades never need `down -v`. After upgrading, replay a match once to fill in the newer fields (rows are updated in place).

## Structure
- `producer/`: Cricsheet parser and Kafka producer
- `spark/`: Kafka consumer (Structured Streaming) that writes to PostgreSQL
- `database/`: PostgreSQL tables and views
- `dashboard/`: Streamlit analytics dashboard
- `data/`: Cricsheet JSON files

## What the dashboard shows
Match info and result (including super overs), and for each innings: score, overs, run rate, a full batting card (dismissal, 4s/6s, strike rate, "did not bat"), extras breakdown, fall of wickets, bowling figures (overs, maidens, runs, wickets, economy, wides, no-balls), runs per over and the latest deliveries.

## Tests
```bash
pip install -r requirements-dev.txt
docker compose up -d postgres        # DB tests are skipped if Postgres is unreachable
pytest
```
Covers the Cricsheet parser (combined extras, multiple wickets per ball, super overs), idempotent/duplicate-safe ingestion, the scorecard SQL views, the bundled matches' official totals, and a headless render of the dashboard for partial and tied matches. Tests run in a temporary schema, so your data is untouched.

## Why replays and restarts are safe
- Every delivery has a unique `event_id` (`<match>-<innings>-<over>-<ball>`) and Spark writes with `INSERT ... ON CONFLICT DO UPDATE`, so receiving the same delivery twice never crashes the stream or creates duplicate rows.
- Spark clears its checkpoint on start-up and re-reads the topic from the beginning. This is safe because of the idempotent writes, and it avoids stale offsets when Kafka's (non-persistent) data is recreated.
- The dashboard closes its database connection on every refresh and gives every table/chart an explicit key.

## Notes
This is a basic educational demo, not a production pipeline. It replays a completed match instead of connecting to a live API and covers common T20 scoring fields. Verify the chosen match's score against its scorecard.
