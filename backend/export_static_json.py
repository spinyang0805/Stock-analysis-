"""export_static_json.py — Export all stock data into static JSON files under public/data/.

The frontend reads these files directly (no API round-trip, no cold start):
  public/data/manifest.json        overall metadata (updated_at, counts, last date)
  public/data/stocklist.json       all products for client-side search
  public/data/prices.json          {code: latest close} for the watchlist page
  public/data/stocks/{code}.json   per-stock bundle {kline, chip, analysis, fundamentals}

Two data sources:
  --source db    read Supabase directly (fast; needs port 5432 open — home / CI)
  --source api   pull from the deployed Fly.io API (works behind firewalls)
  default: try db first, fall back to api.

Usage:
  python export_static_json.py                 # full universe (backend/stocks.txt)
  python export_static_json.py --limit 10      # smoke test
  python export_static_json.py --source api --workers 6
"""
import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BACKEND_DIR)
DEFAULT_OUT = os.path.join(REPO_ROOT, "public", "data")
STOCKS_TXT = os.path.join(BACKEND_DIR, "stocks.txt")
API_BASE = os.environ.get("STOCK_API_BASE", "https://stock-analysis-tw.fly.dev")

_print_lock = threading.Lock()


def log(msg: str):
    with _print_lock:
        print(msg, flush=True)


# ── Universe ─────────────────────────────────────────────────────────────────
def load_env_file():
    """Load backend/.env into os.environ (DATABASE_URL etc.)."""
    env_path = os.path.join(BACKEND_DIR, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def load_universe(source: str, session=None):
    """Codes from stocks.txt + metadata (name/market/industry).

    api mode: backend /api/products/all (merges DB product_universe + open APIs;
              TWSE/TPEx open APIs are often unreachable from office networks).
    db mode:  stock_list.get_all_products (merges DB + open APIs locally).
    """
    products = {}
    if source == "api" and session is not None:
        payload = api_get(session, "/api/products/all")
        products = {p["code"]: p for p in payload.get("items", []) if p.get("code")}
    if not products:
        sys.path.insert(0, BACKEND_DIR)
        from stock_list import get_all_products
        products = {p["code"]: p for p in get_all_products()}
    with open(STOCKS_TXT, encoding="utf-8") as f:
        codes = [c.strip().upper() for c in f if c.strip()]
    universe = []
    for code in codes:
        item = products.get(code) or {"code": code, "name": code, "market": "上市",
                                      "type": "股票", "industry": "股票"}
        universe.append(item)
    return universe, list(products.values())


# ── Payload shrinking ────────────────────────────────────────────────────────
def round_floats(obj, ndigits=4):
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, dict):
        return {k: round_floats(v, ndigits) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_floats(v, ndigits) for v in obj]
    return obj


def write_json(path: str, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), default=str)
    os.replace(tmp, path)


# ── Source: deployed API ─────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=2)
    s.mount("https://", adapter)
    return s


def api_get(session, path, tries=3, timeout=60):
    last = None
    for attempt in range(tries):
        try:
            r = session.get(f"{API_BASE}{path}", timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {path} failed: {last}")


def fetch_bundle_api(session, item):
    code = item["code"]
    kline = api_get(session, f"/api/kline/{code}")
    chip = api_get(session, f"/api/chip/{code}?auto_init=false")
    analysis = api_get(session, f"/api/analysis/{code}")
    fundamentals = api_get(session, f"/api/fundamentals/{code}")
    # Strip response-cache noise; realtime is per-request state, not archive data
    for payload in (kline, chip, analysis):
        payload.pop("cache_hit", None)
        payload.pop("analysis_cache_error", None)
    kline["realtime"] = None
    # Backend meta lacks the product name for most stocks — fill from universe
    for payload in (kline, analysis):
        meta = payload.get("meta") or {}
        if meta.get("name") in (None, "", code):
            meta.update({"name": item.get("name") or code,
                         "market": item.get("market") or meta.get("market") or "--",
                         "industry": item.get("industry") or meta.get("industry") or "--"})
    return {"kline": kline, "chip": chip, "analysis": analysis, "fundamentals": fundamentals}


# ── Source: direct DB ────────────────────────────────────────────────────────
def db_available():
    load_env_file()
    if not os.environ.get("DATABASE_URL"):
        return False
    sys.path.insert(0, BACKEND_DIR)
    try:
        import psycopg2
        conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=8)
        conn.close()
        return True
    except Exception as exc:
        log(f"[db] not reachable: {exc}")
        return False


def fetch_bundle_db(item):
    code = item["code"]
    from firebase_cache import get_valid_stock_daily_series, get_chip_rows, _run
    import payload_builder as pb

    daily_rows = get_valid_stock_daily_series(code, limit=260)
    chip_rows = get_chip_rows(code, limit=60)
    fund_row, _ = _run(
        f"SELECT {', '.join(pb.FUNDAMENTALS_COLS)} FROM fundamentals WHERE stock_id = %s",
        (code,), fetch="one",
    )
    info = {"name": item.get("name"), "market": item.get("market"), "industry": item.get("industry")}
    return {
        "kline": pb.build_kline_payload(code, daily_rows, info),
        "chip": pb.build_chip_payload(code, chip_rows),
        "analysis": pb.build_analysis_payload(code, daily_rows, chip_rows, info),
        "fundamentals": pb.build_fundamentals_payload(code, fund_row),
    }


# ── Main ─────────────────────────────────────────────────────────────────────
def export_stock(fetch_fn, item, out_dir):
    code = item["code"]
    bundle = fetch_fn(item)
    doc = {
        "code": code,
        "name": item.get("name") or code,
        "market": item.get("market") or "--",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        **bundle,
    }
    write_json(os.path.join(out_dir, "stocks", f"{code}.json"), round_floats(doc))
    meta = (bundle.get("kline") or {}).get("meta") or {}
    return code, meta.get("close"), meta.get("data_date")


def main():
    parser = argparse.ArgumentParser(description="Export stock data to static JSON")
    parser.add_argument("--source", choices=["auto", "db", "api"], default="auto")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--limit", type=int, default=0, help="only first N stocks (smoke test)")
    parser.add_argument("--only", default="", help="comma-separated codes to export")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    source = args.source
    if source == "auto":
        source = "db" if db_available() else "api"
    log(f"[export] source={source} out={args.out}")

    session = make_session() if source == "api" else None
    universe, all_products = load_universe(source, session)
    if args.only:
        wanted = {c.strip().upper() for c in args.only.split(",") if c.strip()}
        universe = [u for u in universe if u["code"] in wanted]
    if args.limit:
        universe = universe[:args.limit]
    log(f"[export] universe={len(universe)} stocks, stocklist={len(all_products)} products")

    if source == "db":
        load_env_file()
        fetch_fn = fetch_bundle_db
    else:
        fetch_fn = lambda item: fetch_bundle_api(session, item)

    prices = {}
    last_dates = []
    errors = []
    done = 0
    started = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(export_stock, fetch_fn, item, args.out): item["code"]
                   for item in universe}
        for future in as_completed(futures):
            code = futures[future]
            done += 1
            try:
                code, close, data_date = future.result()
                if close is not None:
                    prices[code] = close
                if data_date:
                    last_dates.append(str(data_date))
            except Exception as exc:
                errors.append({"code": code, "error": str(exc)})
                log(f"[export] {code} FAILED: {exc}")
            if done % 50 == 0 or done == len(universe):
                rate = done / max(1e-9, time.time() - started)
                eta = (len(universe) - done) / max(rate, 1e-9)
                log(f"[export] {done}/{len(universe)} ok={done - len(errors)} "
                    f"err={len(errors)} eta={eta / 60:.1f}m")

    write_json(os.path.join(args.out, "stocklist.json"), all_products)
    write_json(os.path.join(args.out, "prices.json"), round_floats(prices, 2))
    write_json(os.path.join(args.out, "manifest.json"), {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "stock_count": len(universe),
        "exported": len(universe) - len(errors),
        "error_count": len(errors),
        "errors": errors[:30],
        "last_trade_date": max(last_dates) if last_dates else None,
    })
    log(f"[export] done: {len(universe) - len(errors)}/{len(universe)} stocks, "
        f"{len(errors)} errors, {(time.time() - started) / 60:.1f} min")
    return 1 if len(errors) > len(universe) // 2 else 0


if __name__ == "__main__":
    sys.exit(main())
