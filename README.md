# Simple Real-Time Cricket Analytics Project

This college-demo project replays a Cricsheet JSON match through Python, Kafka, PySpark Structured Streaming, PostgreSQL and Streamlit.

## Requirements
- Docker Desktop
- Python 3.10 or 3.11
- Internet for the first Docker build

## Setup
1. Download one T20 JSON match from https://cricsheet.org/downloads/ and save it as `data/match.json`.
2. Start Kafka and PostgreSQL:
   ```bash
   docker compose up -d kafka postgres
   ```
3. Create a virtual environment and install producer dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
   On Windows use `.venv\Scripts\Activate.ps1`.
4. Start Spark:
   ```bash
   docker compose up -d spark
   docker compose logs -f spark
   ```
5. Replay the match in another terminal:
   ```bash
   python producer/replay_cricsheet.py --file data/match.json --delay 2
   ```
6. Start/open the dashboard:
   ```bash
   docker compose up -d dashboard
   ```
   Visit http://localhost:8501.

## Stop
`docker compose down`

To delete database contents too: `docker compose down -v` (this deletes saved data).

## Structure
- `producer/`: Cricsheet parser and Kafka producer
- `spark/`: Kafka consumer and Spark transformations
- `database/`: PostgreSQL tables and views
- `dashboard/`: Streamlit analytics dashboard
- `data/`: Put your downloaded Cricsheet JSON here

## Notes
This is a basic educational demo, not a production pipeline. It replays a completed match instead of connecting to a live API. It covers common T20 scoring fields. Verify the chosen match's score against its scorecard. The simple JDBC append flow is intended for a first demonstration; clear the deliveries table before replaying the same match again to avoid duplicate rows.
