#!/usr/bin/env python3
"""
local_chip_backfill.py — 本機一次回補所有股票籌碼+基本面資料
=================================================================
使用方式:
  cd backend
  set DATABASE_URL=postgresql://...    # Windows
  # export DATABASE_URL=...           # Mac/Linux

  python local_chip_backfill.py               # 籌碼 + 基本面，最近 12 個月
  python local_chip_backfill.py --months 6    # 只補 6 個月籌碼
  python local_chip_backfill.py --chip-only   # 只補籌碼，跳過基本面
  python local_chip_backfill.py --fund-only   # 只補基本面，跳過籌碼
=================================================================
"""
import argparse
import os
import sys
import signal
import time
from datetime import datetime

# ──── 讀取 .env ────────────────────────────────────────
def _load_dotenv():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file):
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

_load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("\n[ERR] DATABASE_URL 環境變數未設定")
    print("   Windows: set DATABASE_URL=postgresql://user:pass@host:5432/dbname")
    sys.exit(1)

print(f"\n[DB] 資料庫：{DATABASE_URL[:50]}{'...' if len(DATABASE_URL) > 50 else ''}")

try:
    from firebase import db
    from firebase_cache import _run, get_all_products_from_db
    from jobs import (
        run_chip_history_backfill,
        write_t86_chips, write_margin_chips,
        write_tpex_insti_chips, write_tpex_margin_chips,
        recent_trading_dates, today_str,
    )
except Exception as e:
    print(f"[ERR] 匯入模組失敗: {e}")
    sys.exit(1)

if db is None:
    print("[ERR] 資料庫連線失敗")
    sys.exit(1)

_STOP = False

def _handle_sigint(sig, frame):
    global _STOP
    print("\n[WARN]  收到中斷，正在完成本批次後停止...")
    _STOP = True
    signal.signal(signal.SIGINT, signal.SIG_DFL)

signal.signal(signal.SIGINT, _handle_sigint)


# ──── 籌碼回補 ────────────────────────────────────────
def run_chip_backfill(months: int = 12, sleep: float = 0.3):
    days = max(20, months * 22)
    dates = recent_trading_dates(days)
    total = len(dates)
    print(f"\n[籌碼] 回補 {total} 個交易日（約 {months} 個月）...")
    print(f"  {'日期':<10}  {'TWSE三大法人':>10}  {'TWSE融資券':>10}  {'TPEx三大法人':>12}  {'TPEx融資券':>10}  {'錯誤'}")
    print("  " + "-" * 75)

    total_t86 = total_margin = total_tpex_t86 = total_tpex_margin = 0
    for i, date_text in enumerate(dates, 1):
        if _STOP:
            break
        per = {"chips": 0, "margin_rows": 0, "errors": []}
        t86    = write_t86_chips(date_text, per)
        margin = write_margin_chips(date_text, per)
        tpex_t = write_tpex_insti_chips(date_text, per)
        tpex_m = write_tpex_margin_chips(date_text, per)
        total_t86        += int(t86 or 0)
        total_margin     += int(margin or 0)
        total_tpex_t86   += int(tpex_t or 0)
        total_tpex_margin += int(tpex_m or 0)
        err_str = f"{len(per['errors'])} 個" if per["errors"] else "無"
        print(f"  {date_text:<10}  {int(t86 or 0):>10}  {int(margin or 0):>10}  {int(tpex_t or 0):>12}  {int(tpex_m or 0):>10}  {err_str}", flush=True)
        time.sleep(sleep)

    print(f"\n  [OK] 籌碼回補完成")
    print(f"     TWSE三大法人合計: {total_t86} 筆")
    print(f"     TWSE融資券合計:   {total_margin} 筆")
    print(f"     TPEx三大法人合計: {total_tpex_t86} 筆")
    print(f"     TPEx融資券合計:   {total_tpex_margin} 筆")


# ──── 基本面回補 ────────────────────────────────────────
def _ensure_fundamentals_table():
    """建立 fundamentals 表（若不存在）。"""
    sql = """
    CREATE TABLE IF NOT EXISTS fundamentals (
        stock_id    VARCHAR(10) NOT NULL,
        date        DATE NOT NULL,
        pe_ratio    NUMERIC(10,2),
        pb_ratio    NUMERIC(10,2),
        dividend_yield NUMERIC(10,4),
        revenue     BIGINT,
        revenue_mom NUMERIC(8,2),
        revenue_yoy NUMERIC(8,2),
        revenue_date VARCHAR(7),
        updated_at  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (stock_id, date)
    );
    CREATE INDEX IF NOT EXISTS idx_fund_stock ON fundamentals(stock_id);
    """
    _, err = _run(sql)
    if err:
        print(f"  [WARN] 建表失敗: {err}")
        return False
    return True


def _fetch_twse_valuation():
    """從 TWSE openapi 取得上市股票 PE/PB/殖利率。"""
    import requests
    try:
        r = requests.get(
            "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d",
            headers={"Accept": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        result = {}
        for row in data:
            code = str(row.get("Code") or row.get("證券代號") or "").strip()
            if not code:
                continue
            def _f(key, alt=None):
                v = row.get(key) or (row.get(alt) if alt else None)
                try:
                    return float(str(v).replace(",", "")) if v not in (None, "", "-", "--") else None
                except Exception:
                    return None
            result[code] = {
                "pe_ratio":       _f("PeRatio",      "本益比"),
                "dividend_yield": _f("DividendYield", "殖利率"),
                "pb_ratio":       _f("PbRatio",      "股價淨值比"),
            }
        return result
    except Exception as e:
        print(f"  [WARN] TWSE valuation API 失敗: {e}")
        return {}


def _fetch_mops_revenue(stock_id: str, offset_months: int = 0):
    """從 MOPS 取得月營收。"""
    import requests
    now = datetime.now()
    m = now.month - offset_months
    y = now.year
    while m <= 0:
        m += 12; y -= 1
    roc_year = y - 1911
    try:
        r = requests.post(
            "https://mops.twse.com.tw/mops/web/ajax_t05st10",
            data={"firstin": "1", "off": "1", "TYPEK": "sii", "year": str(roc_year), "mon": f"{m:02d}"},
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://mops.twse.com.tw/"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        result = {}
        for row in data.get("aaData", []):
            code = str(row[0]).strip() if row else ""
            if not code:
                continue
            def _n(s):
                try: return float(str(s).replace(",", ""))
                except: return None
            rev      = _n(row[2])
            rev_last = _n(row[3])
            rev_yoy  = _n(row[5])
            if rev is not None:
                mom = round((rev / rev_last - 1) * 100, 2) if rev_last else None
                yoy = round((rev / rev_yoy  - 1) * 100, 2) if rev_yoy  else None
                result[code] = {
                    "revenue":     int(rev),
                    "revenue_mom": mom,
                    "revenue_yoy": yoy,
                    "revenue_date": f"{y}-{m:02d}",
                }
        return result
    except Exception as e:
        print(f"  [WARN] MOPS revenue API 失敗 (offset={offset_months}): {e}")
        return {}


def run_fundamentals_backfill(sleep: float = 0.05):
    print(f"\n[基本面] 回補 PE/PB/殖利率 + 月營收...")

    if not _ensure_fundamentals_table():
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # 1. 取得上市股票估值
    print("  取得 TWSE PE/PB/殖利率...")
    valuation = _fetch_twse_valuation()
    print(f"  → {len(valuation)} 筆估值資料")

    # 2. 取得月營收（最近 2 個月）
    print("  取得 MOPS 月營收（當月）...")
    revenue_cur  = _fetch_mops_revenue(offset_months=1)   # 上個月（較穩定）
    print(f"  → {len(revenue_cur)} 筆營收資料")

    # 3. 合併寫入 DB
    all_codes = set(valuation.keys()) | set(revenue_cur.keys())
    written = skipped = 0
    for code in sorted(all_codes):
        val = valuation.get(code, {})
        rev = revenue_cur.get(code, {})
        if not val and not rev:
            skipped += 1
            continue
        sql = """
            INSERT INTO fundamentals
                (stock_id, date, pe_ratio, pb_ratio, dividend_yield,
                 revenue, revenue_mom, revenue_yoy, revenue_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (stock_id, date) DO UPDATE SET
                pe_ratio=EXCLUDED.pe_ratio, pb_ratio=EXCLUDED.pb_ratio,
                dividend_yield=EXCLUDED.dividend_yield,
                revenue=EXCLUDED.revenue, revenue_mom=EXCLUDED.revenue_mom,
                revenue_yoy=EXCLUDED.revenue_yoy, revenue_date=EXCLUDED.revenue_date,
                updated_at=NOW()
        """
        _, err = _run(sql, (
            code, today,
            val.get("pe_ratio"), val.get("pb_ratio"), val.get("dividend_yield"),
            rev.get("revenue"), rev.get("revenue_mom"), rev.get("revenue_yoy"), rev.get("revenue_date"),
        ))
        if err:
            print(f"  [WARN] {code}: {err}")
        else:
            written += 1
        time.sleep(sleep)

    print(f"\n  [OK] 基本面回補完成：寫入 {written} 筆，略過 {skipped} 筆")


# ──── 主程式 ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="本機籌碼+基本面資料回補")
    parser.add_argument("--months",    type=int,   default=12,  help="籌碼回補月數（預設 12）")
    parser.add_argument("--sleep",     type=float, default=0.3, help="每日間隔秒數（預設 0.3）")
    parser.add_argument("--chip-only", action="store_true", help="只補籌碼")
    parser.add_argument("--fund-only", action="store_true", help="只補基本面")
    args = parser.parse_args()

    print("\n" + "=" * 65)
    print("  [CHART] 本機籌碼 + 基本面資料回補工具")
    print(f"  籌碼月數: {args.months}  |  間隔: {args.sleep}s")
    print("=" * 65)

    if not args.fund_only:
        run_chip_backfill(months=args.months, sleep=args.sleep)

    if not args.chip_only:
        run_fundamentals_backfill()

    print("\n[OK] 全部完成！\n")


if __name__ == "__main__":
    main()
