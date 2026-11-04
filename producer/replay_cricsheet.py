import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer


EXTRA_KINDS = ("wides", "noballs", "byes", "legbyes", "penalty")


def parse_wickets(wickets):
    """Every dismissal on a delivery, with the details a scorecard needs."""
    return [
        {
            "player_out": w.get("player_out"),
            "kind": w.get("kind"),
            "fielders": [
                f["name"] for f in w.get("fielders", []) if f.get("name")
            ],
        }
        for w in wickets
    ]


def parse_match_data(match, match_id):
    """Turn a Cricsheet match dict into one event per delivery."""
    teams = match.get("info", {}).get("teams", [])

    for innings_no, innings in enumerate(match.get("innings", []), start=1):
        batting_team = innings.get("team", "")
        bowling_team = next((team for team in teams if team != batting_team), "")
        is_super_over = bool(innings.get("super_over", False))
        for over in innings.get("overs", []):
            over_no = int(over.get("over", 0)) + 1
            for ball_no, delivery in enumerate(over.get("deliveries", []), start=1):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras", {})
                wickets = parse_wickets(delivery.get("wickets", []))
                first_wicket = wickets[0] if wickets else {}
                event = {
                    "event_id": f"{match_id}-{innings_no}-{over_no}-{ball_no}",
                    "match_id": match_id,
                    "innings_number": innings_no,
                    "over_number": over_no,
                    "ball_number": ball_no,
                    "batter": delivery.get("batter"),
                    "bowler": delivery.get("bowler"),
                    "non_striker": delivery.get("non_striker"),
                    "batter_runs": int(runs.get("batter", 0)),
                    "extra_runs": int(runs.get("extras", 0)),
                    "total_runs": int(runs.get("total", 0)),
                    "extras_type": ",".join(sorted(extras)) if extras else None,
                    "is_wicket": bool(wickets),
                    # First dismissal kept in the flat columns for convenience;
                    # the full list (a ball can have several) is in `wickets`.
                    "wicket_type": first_wicket.get("kind"),
                    "player_dismissed": first_wicket.get("player_out"),
                    # JSON-encoded string: Spark parses it as plain text and
                    # PostgreSQL stores it in a JSONB column.
                    "wickets": json.dumps(wickets),
                    "batting_team": batting_team,
                    "bowling_team": bowling_team,
                    "is_super_over": is_super_over,
                    "event_time": None,
                }
                # Runs per extras type, so combined extras (e.g. a no-ball
                # that also went for leg-byes) are never mis-classified.
                for kind in EXTRA_KINDS:
                    event[f"extras_{kind}"] = int(extras.get(kind, 0))
                yield event


def parse_match(path):
    with Path(path).open(encoding="utf-8") as f:
        match = json.load(f)
    return parse_match_data(match, Path(path).stem)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--delay", type=float, default=2.0)
    # docker-compose publishes the broker to the host on 9094 (EXTERNAL listener).
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--topic", default="deliveries")
    args = parser.parse_args()

    if not Path(args.file).exists():
        raise FileNotFoundError(args.file)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        retries=5,
    )
    sent = 0
    try:
        for event in parse_match(args.file):
            event["event_time"] = datetime.now(timezone.utc).isoformat()
            # .get() waits for the broker ack and raises if the send failed;
            # without it a dead broker would silently drop every message.
            producer.send(
                args.topic,
                key=event["match_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            ).get(timeout=30)
            sent += 1
            print(
                f"Sent #{sent}: innings {event['innings_number']}, "
                f"over {event['over_number']}, ball {event['ball_number']}, "
                f"runs {event['total_runs']}"
            )
            time.sleep(max(args.delay, 0))
    finally:
        producer.flush()
        producer.close()
    print(f"Replay complete: {sent} deliveries sent.")


if __name__ == "__main__":
    main()
