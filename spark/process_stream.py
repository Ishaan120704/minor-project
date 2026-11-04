"""Kafka -> PostgreSQL streaming job.

Design notes (why this is restart/replay safe):

* The sink is idempotent: rows are written with
  ``INSERT ... ON CONFLICT (event_id) DO UPDATE``. Seeing the same delivery
  twice (replaying a match, restarting Spark, a retried micro-batch) is
  harmless instead of crashing the stream with a primary-key violation, and
  a replay also upgrades rows stored by an older version of the pipeline.
* The database schema (database/init.sql) is idempotent and is applied on
  start-up, so schema changes never require wiping the Postgres volume.
* The streaming checkpoint is wiped on start-up. Kafka in docker-compose is
  not persisted, so an old checkpoint would hold offsets that no longer exist
  and the query would die with a data-loss error. Because the sink is
  idempotent, re-reading the topic from the beginning is always safe.
* No ``dropDuplicates`` state: de-duplication is done by the database, so
  replaying the same match after clearing the table works.
"""
import os
import shutil
import time

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/cricket")

COLUMNS = [
    "event_id",
    "match_id",
    "innings_number",
    "over_number",
    "ball_number",
    "batter",
    "bowler",
    "non_striker",
    "batter_runs",
    "extra_runs",
    "total_runs",
    "extras_type",
    "is_wicket",
    "wicket_type",
    "player_dismissed",
    "batting_team",
    "bowling_team",
    "event_time",
    "extras_wides",
    "extras_noballs",
    "extras_byes",
    "extras_legbyes",
    "extras_penalty",
    "is_super_over",
    "wickets",
]

# Values used when a field is missing from a message (older producer format).
DEFAULTS = {
    "extras_wides": 0,
    "extras_noballs": 0,
    "extras_byes": 0,
    "extras_legbyes": 0,
    "extras_penalty": 0,
    "is_super_over": False,
    "wickets": "[]",
}

SCHEMA_FILE = os.getenv("SCHEMA_FILE", "/database/init.sql")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("match_id", StringType()),
    StructField("innings_number", IntegerType()),
    StructField("over_number", IntegerType()),
    StructField("ball_number", IntegerType()),
    StructField("batter", StringType()),
    StructField("bowler", StringType()),
    StructField("non_striker", StringType()),
    StructField("batter_runs", IntegerType()),
    StructField("extra_runs", IntegerType()),
    StructField("total_runs", IntegerType()),
    StructField("extras_type", StringType()),
    StructField("is_wicket", BooleanType()),
    StructField("wicket_type", StringType()),
    StructField("player_dismissed", StringType()),
    StructField("batting_team", StringType()),
    StructField("bowling_team", StringType()),
    StructField("event_time", StringType()),
    StructField("extras_wides", IntegerType()),
    StructField("extras_noballs", IntegerType()),
    StructField("extras_byes", IntegerType()),
    StructField("extras_legbyes", IntegerType()),
    StructField("extras_penalty", IntegerType()),
    StructField("is_super_over", BooleanType()),
    StructField("wickets", StringType()),
])

INSERT_SQL = (
    f"INSERT INTO deliveries ({', '.join(COLUMNS)}) VALUES %s "
    "ON CONFLICT (event_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "event_id")
    + ", processed_at = NOW()"
)


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "cricket_analytics"),
        user=os.getenv("POSTGRES_USER", "cricket"),
        password=os.getenv("POSTGRES_PASSWORD", "cricketpass"),
    )


def with_retries(action, what):
    """Run action(conn) in a transaction, retrying if Postgres is not ready."""
    last_error = None
    for attempt in range(1, 6):
        conn = None
        try:
            conn = pg_connect()
            with conn:  # commits on success, rolls back on error
                with conn.cursor() as cur:
                    return action(cur)
        except psycopg2.OperationalError as exc:
            last_error = exc
            print(f"Postgres not reachable for {what} (attempt {attempt}/5): {exc}")
            time.sleep(2 * attempt)
        finally:
            if conn is not None:
                conn.close()
    raise last_error


def apply_schema():
    """Create/upgrade tables and views. The script is idempotent."""
    if not os.path.exists(SCHEMA_FILE):
        print(f"Schema file {SCHEMA_FILE} not found; assuming Postgres init created it.")
        return
    with open(SCHEMA_FILE, encoding="utf-8") as f:
        sql = f.read()
    with_retries(lambda cur: cur.execute(sql), "schema")
    print("Database schema is up to date.")


def prepare_rows(rows):
    """Fill defaults and drop repeated event_ids (last one wins).

    PostgreSQL refuses an upsert that touches the same row twice in a single
    statement, so duplicates inside one batch must be removed first.
    """
    unique = {}
    for row in rows:
        record = dict(zip(COLUMNS, row))
        for column, default in DEFAULTS.items():
            if record[column] is None:
                record[column] = default
        unique[record["event_id"]] = tuple(record[c] for c in COLUMNS)
    return list(unique.values())


def write_rows(rows):
    """Idempotently upsert rows."""
    rows = prepare_rows(rows)
    if not rows:
        return 0
    with_retries(
        lambda cur: execute_values(cur, INSERT_SQL, rows, page_size=500),
        "insert",
    )
    return len(rows)


def save_batch(df, batch_id):
    # Demo-sized data: a micro-batch is a handful of rows, so collecting it
    # to the driver is fine and lets us do a proper upsert.
    rows = [tuple(row[c] for c in COLUMNS) for row in df.select(*COLUMNS).collect()]
    saved = write_rows(rows)
    if saved:
        print(f"Saved micro-batch {batch_id}: {saved} rows")


def main():
    # See module docstring: stale offsets from a previous run are useless.
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    apply_schema()

    spark = (
        SparkSession.builder
        .appName("CricketAnalyticsStreaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        )
        .option("subscribe", os.getenv("KAFKA_TOPIC", "deliveries"))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .filter(col("event_id").isNotNull() & col("match_id").isNotNull())
    )

    query = (
        events.writeStream
        .foreachBatch(save_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
