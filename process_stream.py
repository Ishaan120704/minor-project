
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

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
    StructField("event_time", StringType())
])

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
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    .option("subscribe", "deliveries")
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(
        from_json(col("value").cast("string"), schema).alias("d")
    )
    .select("d.*")
    .filter(
        col("event_id").isNotNull() &
        col("match_id").isNotNull()
    )
    .dropDuplicates(["event_id"])
)


def save_batch(df, batch_id):
    if df.isEmpty():
        return

    output = df.select(
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
        to_timestamp(col("event_time")).alias("event_time")
    )

    (
        output.write
        .format("jdbc")
        .option("url", os.getenv("POSTGRES_URL"))
        .option("dbtable", "deliveries")
        .option("user", os.getenv("POSTGRES_USER", "cricket"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "cricketpass"))
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Saved micro-batch {batch_id}")


query = (
    events.writeStream
    .foreachBatch(save_batch)
    .option(
        "checkpointLocation",
        "/tmp/spark-checkpoints/cricket"
    )
    .start()
)

query.awaitTermination()
