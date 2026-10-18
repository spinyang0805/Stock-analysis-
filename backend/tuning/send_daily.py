"""send_daily.py — morning sender: deliver the report computed last evening at 08:50 Taipei.

  python backend/tuning/send_daily.py [--dir history/out/daily] [--at 08:50] [--no-wait] [--sent FILE]

Only on trading days. The report must be for the previous trading day; otherwise a short warning is
sent instead (Email / Teams only) so a stale report is never presented as today's.
Full report → Email + Teams; short version → LINE (one push per day).
Per-channel delivery results are recorded in --sent (sent_YYYYMMDD.json): a rerun only retries channels
that have not succeeded, so a manual retry never spends LINE quota twice.
After LATE_CUTOFF the subject is marked （延遲） — the market is already open.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"
import notify  # noqa: E402
import trading_calendar as cal  # noqa: E402

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Taipei")
except Exception:  # pragma: no cover
    from datetime import timezone
    TZ = timezone(timedelta(hours=8))

LATE_CUTOFF = (9, 0)
FULL_CHANNELS = ["email", "teams", "telegram"]


def prev_trading_day(today):
    d = today - timedelta(days=1)
    for _ in range(20):
        s = d.strftime("%Y%m%d")
        if cal.is_trading_day(s):
            return s
        d -= timedelta(days=1)
    return None


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(os.path.dirname(BACKEND), "history", "out", "daily"))
    ap.add_argument("--at", default="08:50")
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--force", action="store_true", help="send even on a non-trading day (testing)")
    ap.add_argument("--sent", help="per-channel delivery record (JSON); default <dir>/sent_<today>.json")
    a = ap.parse_args(argv)
    notify.load_env()
    now = datetime.now(TZ)
    today = now.strftime("%Y%m%d")
    cal.load()
    fallback = cal.calendar_source() == "fallback" or not cal.covers(today)
    if not a.force and not cal.is_trading_day(today):
        print(f"{today} is not a trading day; nothing sent")
        return 0
    sent_path = a.sent or os.path.join(a.dir, f"sent_{today}.json")
    sent = load_json(sent_path, {})
    meta = load_json(os.path.join(a.dir, "latest.json"), {})
    if not a.no_wait:
        hh, mm = map(int, a.at.split(":"))
        wait = (now.replace(hour=hh, minute=mm, second=0, microsecond=0) - datetime.now(TZ)).total_seconds()
        if 0 < wait < 3 * 3600:
            print(f"waiting {wait / 60:.0f} min until {a.at}")
            time.sleep(wait)
    late = datetime.now(TZ).time() >= datetime.now(TZ).replace(hour=LATE_CUTOFF[0], minute=LATE_CUTOFF[1]).time()
    expect = prev_trading_day(now)
    note = "\n\n⚠ 交易日曆無法從證交所取得，今日以平日規則判斷交易日。" if fallback else ""
    report = line = None
    if meta.get("as_of") == expect:
        try:
            with open(os.path.join(a.dir, meta["report"]), encoding="utf-8") as f:
                report = f.read()
            with open(os.path.join(a.dir, meta["line"]), encoding="utf-8") as f:
                line = f.read()
        except (OSError, KeyError):
            report = line = None
    if report is None:
        todo = [c for c in ("email", "teams") if sent.get(f"warning_{c}") != "ok"]
        if not todo:
            print("stale/missing warning already sent on every channel")
            return 1
        msg = (f"昨日（{expect}）的推薦報告沒有產生或檔案缺失，最新一份是 {meta.get('as_of') or '無'}。"
               "請查看 GitHub Actions 的 Daily picks 執行紀錄。" + note)
        res = notify.notify("⚠ 每日股票報告未更新", msg, channels=todo)
        for c, v in res.items():
            if v != "skipped":
                sent[f"warning_{c}"] = "ok" if v == "ok" else v
        _save(sent_path, sent)
        print(res)
        return 1
    d = meta["as_of"]
    report += ai_section(a.dir, d)
    subject = f"{'（延遲）' if late else ''}每日股票報告 {d[:4]}/{d[4:6]}/{d[6:]}"
    todo_full = [c for c in FULL_CHANNELS if sent.get(c) != "ok"]
    res = notify.notify(subject, report + note, channels=todo_full) if todo_full else {}
    if sent.get("line") != "ok":
        res.update(notify.notify("", ("（延遲）" if late else "") + line, channels=["line"]))
    for k, v in res.items():
        if v != "skipped":
            sent[k] = "ok" if v == "ok" else v
        print(f"{k}: {v}")
    _save(sent_path, sent)
    failed = [k for k, v in res.items() if v.startswith("error")]
    return 1 if failed else 0


def ai_section(dir_, d):
    """AI commentary: the PC's 3-model version if it was uploaded (ai_<d>.md in release daily-picks),
    else a Groq fallback generated here; empty string if neither works (the report goes out numbers-only)."""
    path = os.path.join(dir_, f"ai_{d}.md")
    if not os.path.exists(path) and os.environ.get("GH_TOKEN"):
        import subprocess
        subprocess.run(["gh", "release", "download", "daily-picks", "-p", f"ai_{d}.md", "-D", dir_, "--clobber"],
                       capture_output=True)
    if not os.path.exists(path):
        try:
            from tuning import ai_commentary
            ai_commentary.main(["--mode", "groq", "--as-of", d, "--dir", dir_])
        except Exception as exc:
            print(f"[send] AI fallback failed: {exc}")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return "\n\n---\n\n" + f.read()
    return ""


def _save(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(path + ".tmp", path)


if __name__ == "__main__":
    sys.exit(main())
