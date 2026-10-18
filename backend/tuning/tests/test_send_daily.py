import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from tuning import send_daily  # noqa: E402


def setup(tmp_path, monkeypatch, as_of, fail=()):
    (tmp_path / "latest.json").write_text(json.dumps({"as_of": as_of, "report": "r.md", "line": "l.txt"}))
    (tmp_path / "r.md").write_text("REPORT", encoding="utf-8")
    (tmp_path / "l.txt").write_text("LINE", encoding="utf-8")
    calls = []

    def fake(subject, body, channels=None):
        calls.append((subject, body, tuple(channels)))
        return {c: ("error: x" if c in fail else "ok") for c in channels}
    monkeypatch.setattr(send_daily.notify, "notify", fake)
    monkeypatch.setattr(send_daily.notify, "load_env", lambda: None)
    monkeypatch.setattr(send_daily.cal, "is_trading_day", lambda d: True)
    monkeypatch.setattr(send_daily, "prev_trading_day", lambda now: "20261016")
    return calls


def test_sends_once_per_channel_and_retries_only_failed(tmp_path, monkeypatch):
    calls = setup(tmp_path, monkeypatch, "20261016", fail=("teams",))
    sent = tmp_path / "sent.json"
    assert send_daily.main(["--dir", str(tmp_path), "--no-wait", "--sent", str(sent)]) == 1
    rec = json.loads(sent.read_text())
    assert rec["email"] == "ok" and rec["line"] == "ok" and rec["teams"].startswith("error")
    calls.clear()
    monkeypatch.setattr(send_daily.notify, "notify",
                        lambda s, b, channels=None: calls.append(tuple(channels)) or {c: "ok" for c in channels})
    assert send_daily.main(["--dir", str(tmp_path), "--no-wait", "--sent", str(sent)]) == 0
    assert calls == [("teams",)]                     # email / LINE not sent twice


def test_stale_report_sends_warning_not_report(tmp_path, monkeypatch):
    calls = setup(tmp_path, monkeypatch, "20261015")
    assert send_daily.main(["--dir", str(tmp_path), "--no-wait", "--sent", str(tmp_path / "s.json")]) == 1
    assert len(calls) == 1 and "未更新" in calls[0][0] and "line" not in calls[0][2]


def test_missing_report_file_sends_warning(tmp_path, monkeypatch):
    calls = setup(tmp_path, monkeypatch, "20261016")
    os.remove(tmp_path / "r.md")
    assert send_daily.main(["--dir", str(tmp_path), "--no-wait", "--sent", str(tmp_path / "s.json")]) == 1
    assert "未更新" in calls[0][0]
