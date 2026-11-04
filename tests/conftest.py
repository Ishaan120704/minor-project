"""Shared fixtures.

DB tests need a reachable PostgreSQL (the docker-compose one works:
`docker compose up -d postgres`, then `pytest`). They run inside a throw-away
schema and are skipped automatically when no database can be reached.
Connection settings come from POSTGRES_HOST/PORT/DB/USER/PASSWORD.
"""
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "spark"))

# The Spark job imports pyspark at module level; the sink logic under test
# does not need it, so stub it. Same for kafka-python if it is not installed.
_STUBS = ["pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"]
try:
    import kafka  # noqa: F401
except ImportError:
    _STUBS.append("kafka")
_installed = [n for n in _STUBS if n not in sys.modules]
for name in _installed:
    sys.modules[name] = MagicMock()

import process_stream as ps  # noqa: E402
from replay_cricsheet import parse_match, parse_match_data  # noqa: E402

# Remove the fakes again: libraries such as plotly/narwhals probe
# sys.modules for "pyspark" and would trip over a MagicMock.
for name in _installed:
    sys.modules.pop(name, None)

psycopg2 = pytest.importorskip("psycopg2")


def ball(batter="A", bowler="X", non_striker="B", batter_runs=0, extras=None, wickets=None):
    """Build one Cricsheet delivery."""
    extras = extras or {}
    extra_total = sum(extras.values())
    delivery = {
        "batter": batter,
        "bowler": bowler,
        "non_striker": non_striker,
        "runs": {
            "batter": batter_runs,
            "extras": extra_total,
            "total": batter_runs + extra_total,
        },
    }
    if extras:
        delivery["extras"] = extras
    if wickets:
        delivery["wickets"] = wickets
    return delivery


def make_match(innings, teams=("T1", "T2"), overs=20):
    """innings: list of (team, deliveries, super_over) -> Cricsheet-style dict."""
    out = []
    for team, deliveries, super_over in innings:
        # group deliveries into overs of 6 *legal* balls
        overs_list, current, legal = [], [], 0
        for d in deliveries:
            current.append(d)
            ex = d.get("extras", {})
            if "wides" not in ex and "noballs" not in ex:
                legal += 1
            if legal == 6:
                overs_list.append(current)
                current, legal = [], 0
        if current:
            overs_list.append(current)
        inn = {
            "team": team,
            "overs": [{"over": i, "deliveries": ds} for i, ds in enumerate(overs_list)],
        }
        if super_over:
            inn["super_over"] = True
        out.append(inn)
    return {
        "info": {
            "teams": list(teams),
            "overs": overs,
            "balls_per_over": 6,
            "match_type": "T20",
            "dates": ["2026-01-01"],
            "venue": "Test Ground",
            "players": {t: [f"{t}-p{i}" for i in range(3)] for t in teams},
        },
        "innings": out,
    }


@pytest.fixture(scope="session")
def pg_schema():
    """Create an isolated schema with the real init.sql applied."""
    params = dict(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "cricket_analytics"),
        user=os.getenv("POSTGRES_USER", "cricket"),
        password=os.getenv("POSTGRES_PASSWORD", "cricketpass"),
        connect_timeout=3,
    )
    try:
        conn = psycopg2.connect(**params)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    schema = f"test_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
        cur.execute((ROOT / "database" / "init.sql").read_text())
    # libpq reads PGOPTIONS, so every connection made by the code under test
    # (Spark sink, dashboard) lands in the test schema.
    old = os.environ.get("PGOPTIONS")
    os.environ["PGOPTIONS"] = f"-csearch_path={schema}"
    yield conn, schema
    if old is None:
        os.environ.pop("PGOPTIONS", None)
    else:
        os.environ["PGOPTIONS"] = old
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


@pytest.fixture
def db(pg_schema):
    """A cursor on an empty deliveries table."""
    conn, schema = pg_schema
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {schema}")
    cur.execute("TRUNCATE deliveries")
    yield cur
    cur.close()


def to_rows(events):
    rows = []
    for e in events:
        e = dict(e)
        e["event_time"] = "2026-01-01T10:00:00+00:00"
        rows.append(tuple(e[c] for c in ps.COLUMNS))
    return rows


@pytest.fixture
def load(db):
    """Load a Cricsheet-style dict (or file path) into the database."""
    def _load(match, match_id="m1"):
        events = (
            list(parse_match(match)) if isinstance(match, (str, Path))
            else list(parse_match_data(match, match_id))
        )
        ps.write_rows(to_rows(events))
        return events
    return _load
