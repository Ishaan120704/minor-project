"""Headless render of the Streamlit dashboard against the test database."""
import json
import os

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from conftest import ROOT, ball, make_match, to_rows  # noqa: E402
from replay_cricsheet import parse_match, parse_match_data  # noqa: E402
import process_stream as ps  # noqa: E402


def render(monkeypatch, data_dir):
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    return at


def texts(at):
    return [s.value for s in at.success] + [i.value for i in at.info]


def test_empty_database_shows_hint(db, monkeypatch, tmp_path):
    at = render(monkeypatch, tmp_path)
    assert any("No match data yet" in t for t in texts(at))


def test_partial_match_progresses(db, monkeypatch, tmp_path):
    events = list(parse_match(ROOT / "data" / "match.json"))
    first = [e for e in events if e["innings_number"] == 1]
    second = [e for e in events if e["innings_number"] == 2]
    (tmp_path / "match.json").write_text((ROOT / "data" / "match.json").read_text())

    ps.write_rows(to_rows(first[:40]))                       # mid first innings
    at = render(monkeypatch, tmp_path)
    assert "Waiting for both innings to be recorded." in texts(at)

    ps.write_rows(to_rows(first[40:] + second[:30]))         # chase under way
    at = render(monkeypatch, tmp_path)
    assert "Second innings is still in progress." in texts(at)

    ps.write_rows(to_rows(second))                           # finished
    at = render(monkeypatch, tmp_path)
    assert "🏆 Australia won by 44 runs!" in texts(at)


def test_selected_match_survives_database_reset(db, monkeypatch, tmp_path):
    ps.write_rows(to_rows(list(parse_match(ROOT / "data" / "match2.json"))))
    at = render(monkeypatch, tmp_path)
    assert at.selectbox[0].value == "match2"
    db.execute("TRUNCATE deliveries")
    at.run()
    assert not at.exception


def test_super_over_decides_a_tie(db, monkeypatch, tmp_path):
    main1 = [ball(batter_runs=1)] * 6                 # T1: 6
    main2 = [ball(batter_runs=1)] * 6                 # T2: 6 -> tie
    so1 = [ball(batter_runs=r) for r in (4, 1, 1, 1, 1, 2)]   # T2 bats first: 10
    so2 = [ball(batter_runs=1)] * 6                   # T1: 6
    match = make_match(
        [("T1", main1, False), ("T2", main2, False), ("T2", so1, True), ("T1", so2, True)],
        overs=1,
    )
    (tmp_path / "tie.json").write_text(json.dumps(match))
    ps.write_rows(to_rows(list(parse_match_data(match, "tie"))))

    at = render(monkeypatch, tmp_path)
    assert "Match tied!" in texts(at)
    assert "🏆 T2 won by 4 runs!" in texts(at)
    assert any("Super Over" in s.value for s in at.subheader)
