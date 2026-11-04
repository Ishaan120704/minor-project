import argparse
import json
import time
from pathlib import Path
from kafka import KafkaProducer


def parse_match(path):
    with Path(path).open(encoding="utf-8") as f:
        match = json.load(f)
    teams = match.get("info", {}).get("teams", [])
    match_id = Path(path).stem

    for innings_no, innings in enumerate(match.get("innings", []), start=1):
        batting_team = innings.get("team", "")
        bowling_team = next((team for team in teams if team != batting_team), "")
        for over in innings.get("overs", []):
            over_no = int(over.get("over", 0)) + 1
            for ball_no, delivery in enumerate(over.get("deliveries", []), start=1):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras", {})
                wickets = delivery.get("wickets", [])
                wicket = wickets[0] if wickets else {}
                yield {
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
                    "wicket_type": wicket.get("kind"),
                    "player_dismissed": wicket.get("player_out"),
                    "batting_team": batting_team,
                    "bowling_team": bowling_team,
                    "event_time": None,
                }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="deliveries")
    args = parser.parse_args()

    if not Path(args.file).exists():
        raise FileNotFoundError(args.file)

    producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    acks="all"
)
    sent = 0
    try:
        for event in parse_match(args.file):
            producer.send(
    args.topic,
    key=event["match_id"].encode("utf-8"),
    value=json.dumps(event).encode("utf-8")
)
            producer.flush()
            sent += 1
            print(f"Sent #{sent}: innings {event['innings_number']}, over {event['over_number']}, ball {event['ball_number']}, runs {event['total_runs']}")
            time.sleep(max(args.delay, 0))
    finally:
        producer.close()
    print(f"Replay complete: {sent} deliveries sent.")


if __name__ == "__main__":
    main()
