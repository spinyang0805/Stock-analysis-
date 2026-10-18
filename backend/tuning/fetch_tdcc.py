"""fetch_tdcc.py — 集保股權分散表（大戶持股）history for the tuning engine. Local Parquet only.

  weekly   : TDCC open data (latest week, all stocks, one CSV download)
  backfill : TDCC qryStock per stock per week (only the last ~51 weeks are served), resumable;
             --limit-stocks N per run so it can be spread over several nights

Output history/tdcc.parquet: stock_id, date (TDCC data date, a Friday-ish), p100 / p400 / p1000 (% of
集保 shares held by holders of >100 / >400 / >1000 張), n_total (holders), n1000 (holders >1000 張).
Levels: 10 = 100,001-200,000 shares … 15 = 1,000,001+ shares (1 張 = 1,000 shares).
In backtests a week's figure is only usable from the NEXT trading day after its data date (published later).

  python backend/tuning/fetch_tdcc.py weekly
  python backend/tuning/fetch_tdcc.py backfill --top 500 [--limit-stocks 100] [--sleep 1.2]
"""
import argparse
import io
import os
import re
import sys
import time

import pandas as pd
import requests

TUNING_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TUNING_DIR)
import data as data_mod  # noqa: E402

PATH = os.path.join(data_mod.HISTORY_DIR, "tdcc.parquet")
COLS = ["stock_id", "date", "p100", "p400", "p1000", "n_total", "n1000"]
URL_Q = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
URL_OD = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
UA = {"User-Agent": "Mozilla/5.0"}


def log(msg):
    print(msg, flush=True)


def summarize(levels):
    """levels: {level int: (people, pct)} → summary row values."""
    pct = lambda ks: round(sum(levels.get(k, (0, 0))[1] for k in ks), 4)  # noqa: E731
    total = levels.get(17, (None, None))[0]
    if total is None:
        total = sum(v[0] for k, v in levels.items() if k <= 15)
    return {"p100": pct(range(10, 16)), "p400": pct(range(12, 16)), "p1000": pct([15]),
            "n_total": total, "n1000": levels.get(15, (0, 0))[0]}


def load():
    return pd.read_parquet(PATH) if os.path.exists(PATH) else pd.DataFrame(columns=COLS)


def save(old, rows):
    if not rows:
        return old
    allv = (pd.concat([old, pd.DataFrame(rows, columns=COLS)], ignore_index=True)
            .drop_duplicates(["stock_id", "date"], keep="last").sort_values(["stock_id", "date"]))
    os.makedirs(os.path.dirname(PATH), exist_ok=True)
    allv.to_parquet(PATH + ".tmp", compression="zstd", index=False)
    os.replace(PATH + ".tmp", PATH)
    log(f"  [store] tdcc.parquet rows={len(allv):,} stocks={allv['stock_id'].nunique()}")
    rows.clear()
    return allv


def cmd_weekly(args):
    r = requests.get(URL_OD, headers=UA, timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")), dtype=str)
    df.columns = ["date", "stock_id", "level", "people", "shares", "pct"]
    df["level"] = df["level"].astype(int)
    rows = []
    for (d, sid), g in df.groupby(["date", "stock_id"]):
        lv = {int(r.level): (float(r.people or 0), float(r.pct or 0)) for r in g.itertuples()}
        rows.append({"stock_id": sid.strip(), "date": d, **summarize(lv)})
    log(f"[weekly] {df['date'].iloc[0]} {len(rows)} stocks")
    save(load(), rows)


def top_codes(n):
    p = data_mod._read_parquet_kind("prices", "20000101", ["stock_id", "date", "turnover", "close", "volume"])
    last = sorted(p["date"].unique())[-60:]
    p = p[p["date"].isin(last) & p["stock_id"].str.fullmatch(r"\d{4}")]
    t = p["turnover"].fillna(p["close"] * p["volume"])
    return list(p.assign(t=t).groupby("stock_id")["t"].mean().sort_values(ascending=False).head(n).index)


def query(sess, sid, d):
    r = sess.get(URL_Q, headers=UA, timeout=60)
    form = dict(re.findall(r'<input[^>]*name="([^"]+)"[^>]*value="([^"]*)"', r.text))
    dates = re.findall(r'<option[^>]*value="(\d{8})"', r.text)
    if d is None:
        return dates
    form.update({"method": "submit", "firDate": d, "scaDate": d, "sqlMethod": "StockNo",
                 "stockNo": sid, "stockName": ""})
    r2 = sess.post(URL_Q, data=form, headers=UA, timeout=60)
    for t in pd.read_html(io.StringIO(r2.text)):
        if t.shape[1] == 5 and len(t) >= 15:
            lv = {}
            for row in t.itertuples(index=False):
                try:
                    lv[int(row[0])] = (float(row[2]) if pd.notna(row[2]) else 0.0, float(row[4]))
                except (ValueError, TypeError):
                    continue
            if lv.get(15) is not None:
                return summarize(lv)
    return None


def cmd_backfill(args):
    sess = requests.Session()
    dates = query(sess, None, None)
    old = load()
    have = set(zip(old["stock_id"], old["date"]))
    codes = top_codes(args.top)
    todo = [s for s in codes if any((s, d) not in have for d in dates)]
    if args.limit_stocks:
        todo = todo[: args.limit_stocks]
    log(f"[backfill] {len(dates)} weeks, {len(codes)} stocks, {len(todo)} to do this run "
        f"(~{len(todo) * len(dates) * (2 * args.sleep + 0.8) / 3600:.1f} h)")
    rows, fails = [], 0
    t_end = time.time() + args.max_minutes * 60 if args.max_minutes else None
    for i, sid in enumerate(todo, 1):
        if t_end and time.time() > t_end:
            log(f"[backfill] 時間到，停在 {i - 1}/{len(todo)}，下次續跑")
            break
        got = 0
        for d in dates:
            if (sid, d) in have:
                continue
            time.sleep(args.sleep)
            try:
                v = query(sess, sid, d)
            except Exception as exc:
                v = None
                log(f"    {sid} {d} error {str(exc)[:60]}")
            if v is None:
                fails += 1
                if fails >= 10:
                    log("[backfill] 連續失敗 10 次，中止（已存部分可續跑）")
                    save(old, rows)
                    return 2
                continue
            fails = 0
            rows.append({"stock_id": sid, "date": d, **v})
            got += 1
        log(f"  [{i}/{len(todo)}] {sid} weeks={got}")
        if i % 5 == 0:
            old = save(old, rows)
    save(old, rows)
    log("[backfill] 完成")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["weekly", "backfill"])
    ap.add_argument("--top", type=int, default=500)
    ap.add_argument("--limit-stocks", type=int)
    ap.add_argument("--sleep", type=float, default=1.2)
    ap.add_argument("--max-minutes", type=float, help="backfill: stop cleanly after this long (resume next run)")
    args = ap.parse_args()
    return {"weekly": cmd_weekly, "backfill": cmd_backfill}[args.kind](args) or 0


if __name__ == "__main__":
    sys.exit(main())
