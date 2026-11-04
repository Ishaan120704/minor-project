import json
from pathlib import Path

import pytest

import process_stream as ps
from conftest import ball, make_match, to_rows
from replay_cricsheet import parse_match, parse_match_data

ROOT = Path(__file__).resolve().parent.parent
REAL_MATCHES = sorted((ROOT / "data").glob("*.json"))

# One over exercising: a boundary, a no-ball+leg-bye combo, a catch, a ball
# with TWO dismissals (non-striker run out + obstructing the field), and a
# retired-hurt (which is not a wicket).
TRICKY_OVER = [
    ball("A", batter_runs=4),
    ball("A", extras={"noballs": 1, "legbyes": 2}),
    ball("A", wickets=[{"player_out": "A", "kind": "caught", "fielders": [{"name": "F"}]}]),
    ball("C", non_striker="B", wickets=[
        {"player_out": "B", "kind": "run out", "fielders": [{"name": "G"}]},
        {"player_out": "C", "kind": "obstructing the field"},
    ]),
    ball("D", wickets=[{"player_out": "D", "kind": "retired hurt"}]),
]


# ----------------------------- parser (no DB) -----------------------------

def test_combined_extras_are_split_per_type():
    events = list(parse_match_data(make_match([("T1", TRICKY_OVER, False)]), "m"))
    combo = events[1]
    assert combo["extras_noballs"] == 1
    assert combo["extras_legbyes"] == 2
    assert combo["extras_wides"] == combo["extras_byes"] == combo["extras_penalty"] == 0
    assert combo["extras_type"] == "legbyes,noballs"


def test_every_wicket_on_a_ball_is_kept():
    events = list(parse_match_data(make_match([("T1", TRICKY_OVER, False)]), "m"))
    wickets = json.loads(events[3]["wickets"])
    assert [w["player_out"] for w in wickets] == ["B", "C"]
    assert wickets[0]["fielders"] == ["G"]
    assert events[3]["player_dismissed"] == "B"  # flat column = first dismissal
    assert json.loads(events[0]["wickets"]) == []


def test_super_over_flag():
    match = make_match([("T1", [ball()] * 6, False), ("T2", [ball()] * 6, True)])
    flags = {e["innings_number"]: e["is_super_over"] for e in parse_match_data(match, "m")}
    assert flags == {1: False, 2: True}


@pytest.mark.parametrize("path", REAL_MATCHES, ids=lambda p: p.name)
def test_event_ids_unique_in_real_matches(path):
    ids = [e["event_id"] for e in parse_match(path)]
    assert len(ids) == len(set(ids)) > 100


# --------------------------- sink / database ---------------------------

def test_replay_is_idempotent(db, load):
    events = load(make_match([("T1", TRICKY_OVER, False)]))
    ps.write_rows(to_rows(events))                      # replay
    ps.write_rows(to_rows(events + events[:3]))         # dups inside one batch
    db.execute("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM deliveries")
    assert db.fetchone() == (len(events), len(events))


def test_replay_upgrades_rows_stored_by_old_version(db, load):
    events = load(make_match([("T1", TRICKY_OVER, False)]))
    # Simulate rows written before the new columns existed.
    db.execute("UPDATE deliveries SET wickets = '[]', extras_noballs = 0, extras_legbyes = 0")
    ps.write_rows(to_rows(events))
    db.execute("SELECT SUM(extras_noballs), SUM(extras_legbyes) FROM deliveries")
    assert db.fetchone() == (1, 2)


def test_missing_new_fields_use_defaults(db):
    row = dict.fromkeys(ps.COLUMNS)
    row.update(event_id="e1", match_id="m", innings_number=1, over_number=1, ball_number=1)
    ps.write_rows([tuple(row[c] for c in ps.COLUMNS)])
    db.execute("SELECT extras_wides, is_super_over, wickets FROM deliveries")
    assert db.fetchone() == (0, False, [])


def test_innings_totals_and_wickets(db, load):
    load(make_match([("T1", TRICKY_OVER, False)]))
    db.execute("""SELECT runs, wickets, legal_balls, extras, noballs, legbyes
                  FROM innings_statistics""")
    # 4 + 3 runs; caught + run out + obstructing = 3 wickets, retired hurt excluded;
    # the no-ball is not a legal ball.
    assert db.fetchone() == (7, 3, 4, 3, 1, 2)


def test_bowling_figures_with_combined_extras(db, load):
    load(make_match([("T1", TRICKY_OVER, False)]))
    db.execute("SELECT runs_conceded, legal_balls, wickets, noballs, economy FROM bowling_statistics")
    runs, legal, wkts, nb, econ = db.fetchone()
    # 7 total - 2 leg-byes (not the bowler's); no-ball run IS charged.
    assert (runs, legal, wkts, nb) == (5, 4, 1, 1)  # run out / obstruction not credited
    assert float(econ) == 7.5


def test_batting_card_dismissals_and_non_striker(db, load):
    load(make_match([("T1", TRICKY_OVER, False)]))
    db.execute("""SELECT batter, how_out, runs, balls_faced, fours
                  FROM batting_statistics ORDER BY batting_position""")
    rows = {r[0]: r[1:] for r in db.fetchall()}
    assert rows["A"] == ("c F b X", 4, 3, 1)            # no-ball counts as a ball faced
    assert rows["B"][0] == "run out (G)"                # never faced a ball, still listed
    assert rows["B"][2] == 0
    assert rows["C"][0] == "obstructing the field"
    assert rows["D"][0] == "retired hurt"


def test_fall_of_wickets(db, load):
    load(make_match([("T1", TRICKY_OVER, False)]))
    db.execute("SELECT wicket_number, player_out, score, over_text FROM fall_of_wickets ORDER BY 1")
    assert db.fetchall() == [
        (1, "A", 7, "0.2"),
        (2, "B", 7, "0.3"),
        (3, "C", 7, "0.3"),
    ]


def test_maiden_over(db, load):
    load(make_match([("T1", [ball()] * 6 + [ball(batter_runs=1)] * 6, False)]))
    db.execute("SELECT maidens, runs_conceded, legal_balls FROM bowling_statistics")
    assert db.fetchone() == (1, 6, 12)


def test_schema_can_be_reapplied_on_populated_database(db, load):
    load(make_match([("T1", TRICKY_OVER, False)]))
    db.execute((ROOT / "database" / "init.sql").read_text())  # what Spark does on start
    db.execute("SELECT COUNT(*) FROM deliveries")
    assert db.fetchone()[0] == 5


EXPECTED = {  # official Cricsheet results for the bundled matches
    "match": {1: (214, 5), 2: (170, 10)},
    "match2": {1: (115, 10), 2: (116, 5)},
}


@pytest.mark.parametrize("path", REAL_MATCHES, ids=lambda p: p.name)
def test_real_match_totals(db, load, path):
    load(path, match_id=path.stem)
    db.execute("""SELECT innings_number, runs, wickets FROM innings_statistics
                  WHERE match_id = %s ORDER BY 1""", (path.stem,))
    got = {n: (int(r), int(w)) for n, r, w in db.fetchall()}
    assert got == EXPECTED[path.stem]
    # batting card must add up to the innings total with extras
    db.execute("""SELECT i.innings_number, i.runs - i.extras, SUM(b.runs)
                  FROM innings_statistics i JOIN batting_statistics b USING (match_id, innings_number)
                  WHERE match_id = %s GROUP BY 1, 2, i.runs, i.extras""", (path.stem,))
    for _, expected, actual in db.fetchall():
        assert expected == actual
    # bowler wickets can never exceed team wickets
    db.execute("""SELECT SUM(wickets) FROM bowling_statistics WHERE match_id = %s
                  GROUP BY innings_number""", (path.stem,))
    assert all(w <= 10 for (w,) in db.fetchall())
