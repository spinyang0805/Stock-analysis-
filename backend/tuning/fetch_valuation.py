"""fetch_valuation.py — daily PE / PB / dividend yield history for the tuning engine.

Sources (by date, full market):
  TWSE BWIBBU_d  (date=YYYYMMDD, selectType=ALL)
  TPEx peQryDate (date=YYYY/MM/DD)
Output: history/valuation_YYYY.parquet (stock_id, date, pe, pb, dy, market). Local only — never writes Supabase.
Trading dates come from the prices store; dates already present are skipped, so the job can be resumed.

  python backend/tuning/fetch_valuation.py --start 20220101 [--end YYYYMMDD] [--sleep 1.5] [--dry-run]
"""
import argparse
import os
import sys
import time

import pandas as pd

TUNING_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(TUNING_DIR)
sys.path.insert(0, TUNING_DIR)
import data as data_mod  # noqa: E402

COLS = ["stock_id", "date", "pe", "pb", "dy", "market"]
FLUSH_EVERY = 20


def log(msg):
    print(msg, flush=True)


def path_for(year):
    return os.path.join(data_mod.HISTORY_DIR, f"valuation_{year}.parquet")


def read_existing():
    frames = [pd.read_parquet(p) for p in
              (path_for(y) for y in range(2000, 2100)) if os.path.exists(p)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS)


def nontrading_dates():
    """manifest.nontrading_dates of the history store (known closed days)."""
    import json
    p = os.path.join(data_mod.HISTORY_DIR, "manifest.json")
    try:
        with open(p, encoding="utf-8") as fh:
            return set(json.load(fh).get("nontrading_dates", []))
    except (OSError, ValueError):
        return set()


def num(x):
    try:
        v = float(str(x).replace(",", "").strip())
    except ValueError:
        return None
    return v if v == v else None


def fetch_day(jobs, d):
    """Returns (rows, status) with status ok | error. Both markets must succeed for the same date."""
    rows = []
    p, err = jobs.fetch_json("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d",
                             params={"date": d, "selectType": "ALL", "response": "json"})
    if isinstance(p, dict) and not err and "沒有符合條件" in str(p.get("stat", "")):
        return [], "closed"
    if err or not isinstance(p, dict) or p.get("stat") != "OK" or str(p.get("date")) != d:
        return [], "error"
    f = p.get("fields") or []
    ix = {k: jobs._idx(f, k) for k in ("證券代號", "本益比", "股價淨值比", "殖利率")}
    for r in p.get("data") or []:
        rows.append({"stock_id": str(r[ix["證券代號"]]).strip(), "date": d, "pe": num(r[ix["本益比"]]),
                     "pb": num(r[ix["股價淨值比"]]), "dy": num(r[ix["殖利率"]]), "market": "TWSE"})
    p2, err2 = jobs.fetch_json("https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate",
                               params={"date": f"{d[:4]}/{d[4:6]}/{d[6:]}", "response": "json"},
                               headers=jobs.TPEX_HEADERS)
    t = (p2.get("tables") or [{}])[0] if isinstance(p2, dict) else {}
    if err2 or not t.get("data") or jobs._roc_any_to_yyyymmdd(t.get("date") or "") != d:
        return [], "error"
    f2 = [str(x).strip() for x in t.get("fields") or []]
    c = {k: jobs._idx(f2, k) for k in ("代號", "本益比", "股價淨值比", "殖利率")}
    for r in t["data"]:
        rows.append({"stock_id": str(r[c["代號"]]).strip(), "date": d, "pe": num(r[c["本益比"]]),
                     "pb": num(r[c["股價淨值比"]]), "dy": num(r[c["殖利率"]]), "market": "TPEx"})
    return rows, "ok"


def flush(existing, buf):
    if not buf:
        return existing
    new = pd.DataFrame(buf, columns=COLS)
    allv = (pd.concat([existing, new], ignore_index=True)
            .drop_duplicates(["stock_id", "date"], keep="last"))
    os.makedirs(data_mod.HISTORY_DIR, exist_ok=True)
    for year in sorted(set(new["date"].str[:4])):
        part = allv[allv["date"].str[:4] == year].sort_values(["stock_id", "date"]).reset_index(drop=True)
        tmp = path_for(year) + ".tmp"
        part.to_parquet(tmp, compression="zstd", index=False)
        os.replace(tmp, path_for(year))
        log(f"  [store] valuation_{year}.parquet rows={len(part):,}")
    buf.clear()
    return allv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20220101")
    ap.add_argument("--end")
    ap.add_argument("--sleep", type=float, default=1.5)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-minutes", type=float, help="stop cleanly after this long (resume next run)")
    args = ap.parse_args()
    t_end = time.time() + args.max_minutes * 60 if args.max_minutes else None

    os.environ.pop("DATABASE_URL", None)  # never touch the DB
    sys.path.insert(0, BACKEND_DIR)
    import jobs

    prices = data_mod._read_parquet_kind("prices", args.start, ["date"])
    closed = nontrading_dates()
    dates = sorted(d for d in prices["date"].unique()
                   if d >= args.start and (not args.end or d <= args.end) and d not in closed)
    existing = read_existing()
    have = set(existing["date"].unique()) if len(existing) else set()
    todo = [d for d in reversed(dates) if d not in have]
    log(f"[valuation] {len(dates)} trading dates, {len(have)} done, {len(todo)} to fetch "
        f"(~{len(todo) * 2 * (args.sleep + 0.7) / 60:.0f} min)")
    if args.dry_run:
        return 0

    real = jobs.fetch_json

    def polite(url, params=None, headers=None, retries=2):
        err = None
        for attempt in range(4):
            time.sleep(args.sleep)
            payload, err = real(url, params=params, headers=headers, retries=retries)
            if not err or payload:
                return payload, err
            time.sleep(5 * 3 ** attempt)
        return {}, err
    jobs.fetch_json = polite

    buf, fails = [], 0
    for i, d in enumerate(todo, 1):
        if t_end and time.time() > t_end:
            log(f"[valuation] 時間到，停在 {i - 1}/{len(todo)}，下次續跑")
            break
        rows, status = fetch_day(jobs, d)
        if status == "closed":
            log(f"  [{i}/{len(todo)}] {d} 交易所回無資料（休市殘留日），略過")
            continue
        if status != "ok":
            fails += 1
            log(f"  [{i}/{len(todo)}] {d} 失敗 (連續 {fails})")
            if fails >= 5:
                log("[valuation] 連續失敗 5 次，中止（已存部分可續跑）")
                break
            continue
        fails = 0
        buf.extend(rows)
        log(f"  [{i}/{len(todo)}] {d} rows={len(rows)}")
        if i % FLUSH_EVERY == 0:
            existing = flush(existing, buf)
    flush(existing, buf)
    log("[valuation] 完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
