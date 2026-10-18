"""fetch_extra.py — extra history for the tuning engine (local Parquet only, never writes Supabase).

  market  : TAIEX (TWSE MI_5MINS_HIST, monthly) + TPEx index (indexInfo/inx, monthly) → history/market_index.parquet
  revenue : MOPS monthly revenue HTML (t21sc03, 上市 sii + 上櫃 otc)                  → history/revenue.parquet
            avail_date = 11th of the following month (legal deadline is the 10th) → no lookahead in backtests
  sbl     : 借券賣出餘額 TWSE TWT93U + TPEx margin/sbl, daily                         → history/sbl_YYYY.parquet

Each kind skips what is already stored, so runs can be resumed.
  python backend/tuning/fetch_extra.py market|revenue|sbl --start 20220101 [--sleep 2]
"""
import argparse
import io
import os
import sys
import time
from datetime import date

import pandas as pd

TUNING_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(TUNING_DIR)
sys.path.insert(0, TUNING_DIR)
import data as data_mod  # noqa: E402

H = data_mod.HISTORY_DIR


def log(msg):
    print(msg, flush=True)


def num(x):
    try:
        v = float(str(x).replace(",", "").strip())
    except ValueError:
        return None
    return v if v == v else None


def atomic_parquet(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    df.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, path)


def months(start, end):
    y, m = int(start[:4]), int(start[4:6])
    out = []
    while (y, m) <= (int(end[:4]), int(end[4:6])):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def setup_jobs(sleep):
    os.environ.pop("DATABASE_URL", None)
    sys.path.insert(0, BACKEND_DIR)
    import jobs
    real = jobs.fetch_json

    def polite(url, params=None, headers=None, retries=2):
        err = None
        for attempt in range(4):
            time.sleep(sleep)
            payload, err = real(url, params=params, headers=headers, retries=retries)
            if not err or payload:
                return payload, err
            time.sleep(5 * 3 ** attempt)
        return {}, err
    jobs.fetch_json = polite
    return jobs


# ── market index ────────────────────────────────────────────────────────────
def cmd_market(args, jobs):
    path = os.path.join(H, "market_index.parquet")
    old = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(columns=["index", "date"])
    today = date.today().strftime("%Y%m%d")
    done = {(r["index"], r["date"][:6]) for _, r in old.iterrows()}
    rows = []
    recent = {f"{y}{m:02d}" for y, m in months(args.start, today)[-2:]}  # this + previous month: always refresh
    for y, m in months(args.start, today):
        ym = f"{y}{m:02d}"
        current = ym in recent
        if ("TAIEX", ym) not in done or current:
            p, err = jobs.fetch_json("https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST",
                                     params={"date": f"{ym}01", "response": "json"})
            for r in (p or {}).get("data") or []:
                d = jobs._roc_any_to_yyyymmdd(r[0])
                if d:
                    rows.append({"index": "TAIEX", "date": d, "open": num(r[1]), "high": num(r[2]),
                                 "low": num(r[3]), "close": num(r[4])})
        if ("TPEX", ym) not in done or current:
            p, err = jobs.fetch_json("https://www.tpex.org.tw/www/zh-tw/indexInfo/inx",
                                     params={"date": f"{y}/{m:02d}/01", "response": "json"},
                                     headers=jobs.TPEX_HEADERS)
            t = (p.get("tables") or [{}])[0] if isinstance(p, dict) else {}
            for r in t.get("data") or []:
                d = str(r[0]).replace("/", "")
                if len(d) == 8 and d.isdigit():
                    rows.append({"index": "TPEX", "date": d, "open": num(r[1]), "high": num(r[2]),
                                 "low": num(r[3]), "close": num(r[4])})
        log(f"  {ym} rows so far {len(rows)}")
    allv = (pd.concat([old, pd.DataFrame(rows)], ignore_index=True)
            .drop_duplicates(["index", "date"], keep="last").sort_values(["index", "date"]))
    atomic_parquet(allv, path)
    log(f"[market] {path} rows={len(allv):,} ({allv.groupby('index').size().to_dict()})")


# ── monthly revenue ─────────────────────────────────────────────────────────
def parse_revenue_html(text, y, m, market):
    out = []
    for t in pd.read_html(io.StringIO(text)):
        if t.shape[1] < 10:
            continue
        t.columns = [" ".join(str(x) for x in c) if isinstance(c, tuple) else str(c) for c in t.columns]
        cid = next((c for c in t.columns if "代號" in c), None)
        cur = next((c for c in t.columns if "當月營收" in c), None)
        yoy = next((c for c in t.columns if "去年同月" in c and "增減" in c), None)
        mom = next((c for c in t.columns if "上月比較" in c), None)
        cum_yoy = next((c for c in t.columns if "前期比較" in c), None)
        if not (cid and cur):
            continue
        for _, r in t.iterrows():
            sid = str(r[cid]).strip()
            if not sid.isdigit() and not sid[:4].isdigit():
                continue
            out.append({"stock_id": sid, "ym": f"{y}{m:02d}", "revenue": num(r[cur]),
                        "yoy": num(r[yoy]) if yoy else None, "mom": num(r[mom]) if mom else None,
                        "cum_yoy": num(r[cum_yoy]) if cum_yoy else None, "market": market})
    return out


def cmd_revenue(args, jobs):
    import requests
    path = os.path.join(H, "revenue.parquet")
    old = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(columns=["ym", "market"])
    done = set(zip(old["ym"], old["market"]))
    today = date.today()
    # include 13 months before start so YoY-derived signals have history at the start date
    s = f"{int(args.start[:4]) - 1}{args.start[4:6]}"
    rows = []
    sess = requests.Session()
    all_m = months(s, today.strftime("%Y%m"))
    recent = {f"{y}{m:02d}" for y, m in all_m[-4:]}   # re-fetch last ~3 published months (late filers / amendments)
    for y, m in all_m:
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        if date(ny, nm, 11) > today:
            continue                                   # not published yet
        for mk, tag in (("TWSE", "sii"), ("TPEx", "otc")):
            if (f"{y}{m:02d}", mk) in done and f"{y}{m:02d}" not in recent:
                continue
            url = f"https://mopsov.twse.com.tw/nas/t21/{tag}/t21sc03_{y - 1911}_{m}_0.html"
            time.sleep(args.sleep)
            try:
                r = sess.get(url, timeout=40, headers={"User-Agent": "Mozilla/5.0"})
                r.encoding = "big5"
                got = parse_revenue_html(r.text, y, m, mk) if r.status_code == 200 else []
            except Exception as exc:
                log(f"  {y}{m:02d} {mk} 失敗 {str(exc)[:80]}")
                continue
            # avail_date: the 11th of the next month; a stock first seen on a later re-fetch (late filer)
            # keeps the date it was first observed, so backtests never use it before it was public
            first_seen = {} if old.empty else {
                r.stock_id: r.avail_date for r in old[old["ym"] == f"{y}{m:02d}"].itertuples()}
            late = max(f"{ny}{nm:02d}11", today.strftime("%Y%m%d"))
            for g in got:
                g["avail_date"] = first_seen.get(g["stock_id"]) or (
                    f"{ny}{nm:02d}11" if (f"{y}{m:02d}", mk) not in done else late)
            rows.extend(got)
            log(f"  {y}{m:02d} {mk} rows={len(got)}")
    allv = (pd.concat([old, pd.DataFrame(rows)], ignore_index=True)
            .drop_duplicates(["stock_id", "ym"], keep="last").sort_values(["stock_id", "ym"]))
    atomic_parquet(allv, path)
    log(f"[revenue] {path} rows={len(allv):,}")


# ── 借券賣出餘額 ─────────────────────────────────────────────────────────────
def sbl_day(jobs, d):
    rows = []
    p, err = jobs.fetch_json("https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U",
                             params={"date": d, "response": "json"})
    if isinstance(p, dict) and not err and "沒有符合條件" in str(p.get("stat", "")):
        return [], "closed"
    if err or not isinstance(p, dict) or p.get("stat") != "OK" or str(p.get("date")) != d:
        return [], "error"
    for r in p.get("data") or []:
        if len(r) > 12:
            rows.append({"stock_id": str(r[0]).strip(), "date": d, "sbl_balance": num(r[12]),
                         "sbl_sell": num(r[9]), "market": "TWSE"})
    p2, err2 = jobs.fetch_json("https://www.tpex.org.tw/www/zh-tw/margin/sbl",
                               params={"date": f"{d[:4]}/{d[4:6]}/{d[6:]}", "response": "json"},
                               headers=jobs.TPEX_HEADERS)
    t = (p2.get("tables") or [{}])[0] if isinstance(p2, dict) else {}
    if err2 or not t.get("data") or jobs._roc_any_to_yyyymmdd(t.get("date") or "") != d:
        return [], "error"
    for r in t["data"]:
        if len(r) > 12:
            rows.append({"stock_id": str(r[0]).strip(), "date": d, "sbl_balance": num(r[12]),
                         "sbl_sell": num(r[9]), "market": "TPEx"})
    return rows, "ok"


def cmd_sbl(args, jobs):
    import glob
    frames = [pd.read_parquet(p) for p in sorted(glob.glob(os.path.join(H, "sbl_*.parquet")))]
    existing = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["stock_id", "date"])
    have = set(existing["date"].unique())
    from fetch_valuation import nontrading_dates
    closed = nontrading_dates()
    dates = sorted(d for d in data_mod._read_parquet_kind("prices", args.start, ["date"])["date"].unique()
                   if d not in closed)
    todo = [d for d in reversed(dates) if d not in have]
    log(f"[sbl] {len(todo)} dates to fetch (~{len(todo) * 2 * (args.sleep + 0.7) / 60:.0f} min)")
    buf, fails = [], 0

    def flush(existing):
        if not buf:
            return existing
        allv = (pd.concat([existing, pd.DataFrame(buf)], ignore_index=True)
                .drop_duplicates(["stock_id", "date"], keep="last"))
        for yr in sorted({r["date"][:4] for r in buf}):
            part = allv[allv["date"].str[:4] == yr].sort_values(["stock_id", "date"])
            atomic_parquet(part, os.path.join(H, f"sbl_{yr}.parquet"))
            log(f"  [store] sbl_{yr}.parquet rows={len(part):,}")
        buf.clear()
        return allv

    t_end = time.time() + args.max_minutes * 60 if args.max_minutes else None
    for i, d in enumerate(todo, 1):
        if t_end and time.time() > t_end:
            log(f"[sbl] 時間到，停在 {i - 1}/{len(todo)}，下次續跑")
            break
        rows, st = sbl_day(jobs, d)
        if st == "closed":
            log(f"  [{i}/{len(todo)}] {d} 交易所回無資料（休市殘留日），略過")
            continue
        if st != "ok":
            fails += 1
            log(f"  [{i}/{len(todo)}] {d} 失敗 (連續 {fails})")
            if fails >= 5:
                log("[sbl] 連續失敗 5 次，中止（已存部分可續跑）")
                break
            continue
        fails = 0
        buf.extend(rows)
        log(f"  [{i}/{len(todo)}] {d} rows={len(rows)}")
        if i % 20 == 0:
            existing = flush(existing)
    flush(existing)
    log("[sbl] 完成")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["market", "revenue", "sbl"])
    ap.add_argument("--start", default="20220101")
    ap.add_argument("--sleep", type=float, default=2.0)
    ap.add_argument("--max-minutes", type=float, help="sbl: stop cleanly after this long (resume next run)")
    args = ap.parse_args()
    jobs = setup_jobs(args.sleep)
    {"market": cmd_market, "revenue": cmd_revenue, "sbl": cmd_sbl}[args.kind](args, jobs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
