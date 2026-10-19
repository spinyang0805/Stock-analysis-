"""holdings_report.py — the user's own holdings analysis (private): compute and optionally email it.

  python backend/tuning/holdings_report.py --source parquet [--send] [--out FILE]

Uses Supabase `transactions` (+ holding_settings style) for shares / average cost, the tuned params for each
stock's exit rule, and the latest bars for trend / trailing stop. The text is never written to a public place.
"""
import argparse
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning import positions, score_today  # noqa: E402
from tuning.data import connect_readonly, log  # noqa: E402


def load_positions():
    conn = connect_readonly()
    try:
        tx, style = positions.load_transactions(conn)
    finally:
        conn.rollback()
        conn.close()
    return positions.positions(tx), style


def advise_all(out, prep, params_path, pos=None, style=None):
    """Holdings advice from an already computed score_today result (out, prep) — used by daily_picks."""
    import json
    if pos is None:
        pos, style = load_positions()
    with open(params_path, encoding="utf-8") as f:
        params = {p["stock_id"]: p for p in json.load(f)["stocks"]}
    alerts = {s["stock_id"]: s for s in out["sell_alerts"]}
    adv = []
    for sid, p in pos.items():
        g = prep["adjusted"].get(sid)
        if g is None or len(g) < 60:
            log(f"[holdings] {sid}: no price history, skipped")
            continue
        adv.append(positions.advise(p, g.reset_index(drop=True), params.get(sid), (style or {}).get(sid),
                                    alerts.get(sid), out["intraday_levels"].get(sid)))
    return adv


def build(sargs):
    pos, style = load_positions()
    out, prep = score_today.run(sargs, extra_codes=list(pos), return_prep=True)
    return out["as_of"], advise_all(out, prep, sargs.params, pos, style)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="email it (email channel only)")
    ap.add_argument("--out")
    known, rest = ap.parse_known_args(argv)
    sargs = score_today.parse_args(rest + ["--include-unreliable", "--out-dir",
                                           os.path.join(os.environ.get("TEMP", "/tmp"), "holdings_tmp")])
    as_of, adv = build(sargs)
    text = positions.render(adv, as_of)
    if known.out:
        with open(known.out, "w", encoding="utf-8") as f:
            f.write(text)
    print(text)
    if known.send:
        import notify
        notify.load_env()
        d = f"{as_of[:4]}/{as_of[4:6]}/{as_of[6:]}"
        print(notify.notify(f"我的持股建議 {d}", text, channels=["email"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
