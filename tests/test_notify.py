import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import notify  # noqa: E402

ALL_VARS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "SMTP_USER", "SMTP_APP_PASSWORD",
            "NOTIFY_EMAIL_TO", "TEAMS_WEBHOOK_URL")


def _clear(monkeypatch):
    for v in ALL_VARS:
        monkeypatch.delenv(v, raising=False)


def test_split_text_respects_limit():
    text = "\n".join(f"line {i} " + "x" * 50 for i in range(300))
    chunks = notify.split_text(text, limit=500)
    assert all(len(c) <= 500 for c in chunks)
    assert "".join(chunks) == text


def test_split_text_hard_cuts_long_line():
    chunks = notify.split_text("a" * 1200, limit=500)
    assert [len(c) for c in chunks] == [500, 500, 200]


def test_unconfigured_channels_are_skipped(monkeypatch):
    _clear(monkeypatch)
    assert notify.notify("s", "b") == {"telegram": "skipped", "email": "skipped", "teams": "skipped"}


def test_one_channel_failure_does_not_block_others(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TEAMS_WEBHOOK_URL", "https://example.invalid/hook")
    sent = []

    def boom(subject, body):
        raise RuntimeError("telegram down")
    monkeypatch.setitem(notify.SENDERS, "telegram", boom)
    monkeypatch.setitem(notify.SENDERS, "teams", lambda s, b: sent.append((s, b)))
    res = notify.notify("subj", "body")
    assert res["telegram"].startswith("error: telegram down")
    assert res["teams"] == "ok" and sent == [("subj", "body")]
    assert res["email"] == "skipped"


def test_email_html_is_escaped():
    out = notify.md_to_html("<script>x</script> & 1")
    assert "<script>" not in out and "&lt;script&gt;" in out and "&amp;" in out
