#!/usr/bin/env python3
"""
local_backfill.py — 本機一次回補所有股票歷史資料
=======================================================
使用方式:
  cd backend
  pip install -r requirements.txt      # 第一次需要
  set DATABASE_URL=postgresql://...    # Windows
  # export DATABASE_URL=postgresql://... # Mac/Linux

  python local_backfill.py             # 全部股票（上市+上櫃），13個月
  python local_backfill.py --months 6  # 只補 6 個月
  python local_backfill.py --market twse  # 只補上市
  python local_backfill.py --market tpex  # 只補上櫃
  python local_backfill.py --force     # 強制重補（忽略已有資料）
  python local_backfill.py --no-daily  # 跳過每日更新步驟
=======================================================
"""
import argparse
import os
import signal
import sys
import time
from datetime import datetime

# ──── 讀取 .env（選用）────────────────────────────────
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

# ──── 環境變數檢查 ────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("\n❌ DATABASE_URL 環境變數未設定")
    print("   請先執行：")
    print("   Windows: set DATABASE_URL=postgresql://user:pass@host:5432/dbname")
    print("   Mac/Linux: export DATABASE_URL=postgresql://user:pass@host:5432/dbname")
    print("\n   或在 backend/ 目錄建立 .env 檔案，內容：")
    print("   DATABASE_URL=postgresql://user:pass@host:5432/dbname\n")
    sys.exit(1)

print(f"\n🔗 資料庫：{DATABASE_URL[:50]}{'...' if len(DATABASE_URL)>50 else ''}")

# ──── 匯入後端模組 ────────────────────────────────────
try:
    from firebase import db, _pool
except Exception as e:
    print(f"❌ 匯入 firebase 模組失敗: {e}")
    sys.exit(1)

if db is None:
    print("❌ 資料庫連線失敗，請確認 DATABASE_URL 和網路設定")
    sys.exit(1)

from jobs import (
    run_on_demand_backfill,
    run_daily_update,
    latest_twse_daily_rows,
    latest_tpex_daily_rows,
    _parse_twse_all_row,
    _parse_tpex_row,
    today_str,
)
from firebase_cache import (
    get_all_products_from_db,
    get_cache_status,
    save_product,
    _run,
)

# ──── 全域中斷旗標 ────────────────────────────────────
_STOP = False

def _handle_sigint(sig, frame):
    global _STOP
    print("\n\n⚠️  收到中斷，等本支股票完成後停止（再按一次強制中斷）...")
    _STOP = True
    signal.signal(signal.SIGINT, signal.SIG_DFL)

signal.signal(signal.SIGINT, _handle_sigint)

# ──── 股票清單 ─────────────────────────────────────────
def get_stock_universe(market_filter: str = "all") -> list:
    """取得完整股票清單，優先從資料庫，否則從 TWSE/TPEx API 爬取。"""
    print("\n[1] 取得股票清單...")

    # 試從資料庫取得
    db_products = get_all_products_from_db(limit=12000)
    if len(db_products) >= 200:
        print(f"    ✅ 從資料庫取得 {len(db_products)} 支股票")
        if market_filter == "twse":
            filtered = [p for p in db_products if p.get("market") in ("上市", "TWSE")]
            if filtered:
                return filtered
        elif market_filter == "tpex":
            filtered = [p for p in db_products if p.get("market") in ("上櫃", "TPEx", "OTC")]
            if filtered:
                return filtered
            print(f"    ⚠️ 資料庫無上櫃股票記錄，改從 TPEx API 爬取清單...")
        else:
            return db_products

    # 從 API 爬取
    stocks = []
    if market_filter in ("all", "twse"):
        date, rows, fields, errs = latest_twse_daily_rows(max_lookback_days=10)
        print(f"    TWSE {date}: {len(rows)} 支")
        for row in rows:
            try:
                code, doc = _parse_twse_all_row(row, fields)
                if code and code.isdigit() and len(code) >= 4:
                    item = {
                        "code": code,
                        "name": doc.get("name") or code,
                        "market": "上市",
                        "type": "股票",
                    }
                    stocks.append(item)
                    save_product(code, item)
            except Exception:
                continue

    if market_filter in ("all", "tpex"):
        date_t, rows_t, fields_t, errs_t = latest_tpex_daily_rows(max_lookback_days=10)
        print(f"    TPEx {date_t}: {len(rows_t)} 支")
        tpex_ok = False
        for row in rows_t:
            try:
                code, doc = _parse_tpex_row(row, fields_t)
                if code and code[:1].isdigit() and len(code) >= 4:
                    item = {
                        "code": code,
                        "name": doc.get("name") or code,
                        "market": "上櫃",
                        "type": "股票",
                    }
                    stocks.append(item)
                    save_product(code, item)
                    tpex_ok = True
            except Exception:
                continue

        # TPEx API 失敗時改用 TWSE 開放資料 API 取得上櫃清單
        if not tpex_ok:
            print("    ⚠️ TPEx API 無回應，改用 TWSE 開放資料取得上櫃清單...")
            try:
                import requests
                res = requests.get(
                    "https://openapi.twse.com.tw/v1/opendata/t187ap04_L",
                    headers={"Accept": "application/json"},
                    timeout=30,
                )
                res.raise_for_status()
                data = res.json()
                count = 0
                for entry in data:
                    code = str(entry.get("公司代號") or entry.get("SecuritiesCompanyCode") or "").strip()
                    name = str(entry.get("公司簡稱") or entry.get("CompanyName") or code).strip()
                    if code and code.isdigit() and len(code) >= 4:
                        item = {"code": code, "name": name, "market": "上櫃", "type": "股票"}
                        stocks.append(item)
                        save_product(code, item)
                        count += 1
                print(f"    ✅ 開放資料取得上櫃股票 {count} 支")
            except Exception as e:
                print(f"    ❌ 開放資料也失敗：{e}")

    # 去重
    seen, result = set(), []
    for s in stocks:
        if s["code"] not in seen:
            seen.add(s["code"])
            result.append(s)

    print(f"    ✅ 合計 {len(result)} 支股票")
    return result


# ──── 一次撈出有缺資料的股票 ────────────────────────────
def get_missing_stocks(all_stocks: list, min_rows: int, min_chip_rows: int = 30) -> list:
    """一次 SQL 取得所有股票的 stock_daily 和 chip_daily 筆數，回傳任一不足門檻的股票。"""
    print(f"\n[計算缺資料] 一次查詢所有股票筆數（K線<{min_rows} 或籌碼<{min_chip_rows}）...")
    daily_rows, e1 = _run(
        "SELECT stock_id, COUNT(*) FROM stock_daily GROUP BY stock_id",
        fetch="all",
    )
    chip_rows, e2 = _run(
        "SELECT stock_id, COUNT(*) FROM chip_daily GROUP BY stock_id",
        fetch="all",
    )
    if e1:
        print(f"  ⚠️ stock_daily 查詢失敗：{e1}，改為逐支檢查")
        return all_stocks

    daily_counts = {r[0]: int(r[1]) for r in (daily_rows or [])}
    chip_counts  = {r[0]: int(r[1]) for r in (chip_rows  or [])}

    missing = []
    for s in all_stocks:
        code = s["code"]
        dcnt = daily_counts.get(code, 0)
        ccnt = chip_counts.get(code, 0)
        if dcnt < min_rows or ccnt < min_chip_rows:
            missing.append({**s, "existing_rows": dcnt, "existing_chip": ccnt})

    # 最缺的排前面（K線 + 籌碼總筆數最少）
    missing.sort(key=lambda x: x.get("existing_rows", 0) + x.get("existing_chip", 0))
    lack_daily = sum(1 for s in missing if s["existing_rows"] < min_rows)
    lack_chip  = sum(1 for s in missing if s["existing_chip"] < min_chip_rows)
    print(f"  ✅ 共 {len(all_stocks)} 支，其中 {len(missing)} 支有缺資料")
    print(f"     K線不足: {lack_daily} 支  |  籌碼不足: {lack_chip} 支")
    return missing


# ──── 主程式 ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="本機股票歷史資料回補工具")
    parser.add_argument("--months",       type=int,   default=13,    help="回補幾個月（預設 13）")
    parser.add_argument("--min-rows",     type=int,   default=90,    help="已有幾筆資料就略過（預設 90）")
    parser.add_argument("--sleep",        type=float, default=0.3,   help="每支股票間隔秒數（預設 0.3）")
    parser.add_argument("--market",       type=str,   default="all", choices=["all","twse","tpex"], help="上市/上櫃/全部")
    parser.add_argument("--force",        action="store_true", help="強制重補（忽略已有資料）")
    parser.add_argument("--no-daily",     action="store_true", help="跳過每日更新步驟")
    parser.add_argument("--missing-only", action="store_true", help="只補有缺資料的股票（一次查詢，不逐支掃描）")
    parser.add_argument("--start-from",  type=str,   default="",   help="從某支股票代號開始（斷點續傳）")
    args = parser.parse_args()

    print("\n" + "=" * 65)
    print("  📈 本機股票歷史資料回補工具")
    print(f"  回補月數: {args.months}  |  略過門檻: {args.min_rows} 筆  |  市場: {args.market}")
    if args.force:
        print("  ⚠️  force 模式：忽略已有資料，全部重新抓取")
    if args.missing_only:
        print("  🔍 missing-only 模式：只補有缺資料的股票")
    print("=" * 65)

    # ── Step 1: 每日更新（補最近 5 個交易日）──────────────
    if not args.no_daily:
        print("\n[2] 補回最近 5 個交易日所有股票資料...")
        try:
            t0 = time.time()
            r = run_daily_update(lookback_days=5)
            print(f"    ✅ 寫入 {r.get('stocks', 0)} 筆  "
                  f"| 日期涵蓋: {r.get('dates_written', [])[:3]}  "
                  f"| 耗時 {time.time()-t0:.1f}s")
            if r.get("errors"):
                print(f"    ⚠️ 有 {len(r['errors'])} 個錯誤（不影響繼續）")
        except Exception as e:
            print(f"    ⚠️ 每日更新發生例外: {e}（繼續執行）")
    else:
        print("\n[2] 已跳過每日更新步驟")

    # ── Step 2: 取得股票清單 ───────────────────────────────
    stocks = get_stock_universe(args.market)
    if not stocks:
        print("❌ 無法取得股票清單，請檢查網路或 API 狀態")
        sys.exit(1)

    # missing-only 模式：一次查詢過濾，只留 K線 不足的股票（籌碼由 local_chip_backfill.py 負責）
    if args.missing_only and not args.force:
        stocks = get_missing_stocks(stocks, args.min_rows, min_chip_rows=0)
        if not stocks:
            print("✅ 所有股票資料都已足夠，不需補充！")
            sys.exit(0)

    # 斷點續傳
    if args.start_from:
        codes = [s["code"] for s in stocks]
        try:
            idx = codes.index(args.start_from)
            stocks = stocks[idx:]
            print(f"    ▶  從 {args.start_from} 開始，剩 {len(stocks)} 支")
        except ValueError:
            print(f"    ⚠️  找不到 {args.start_from}，從頭開始")

    # ── Step 3: 逐支回補 ─────────────────────────────────
    total = len(stocks)
    ok_count = skip_count = err_count = 0
    total_written = 0
    start_time = time.time()

    print(f"\n[3] 逐支回補歷史資料（共 {total} 支）...\n")
    print(f"  {'#':<5} {'代號':<6} {'名稱':<10} {'市場':<5} {'K線':>5} {'籌碼':>5} {'新增':>5}  狀態")
    print("  " + "-" * 68)

    for i, item in enumerate(stocks, start=1):
        if _STOP:
            print(f"\n⛔ 已在第 {i} 支停止（{item['code']}）", flush=True)
            break

        code   = item["code"]
        name   = (item.get("name") or code)[:8]
        market = item.get("market", "上市")
        market_key = "TPEx" if market in ("上櫃", "TPEx", "OTC") else "TWSE"

        # 每10支顯示一次目前掃描的股票（即使略過）
        if i % 10 == 0:
            pct = i / total * 100
            elapsed_s = time.time() - start_time
            rate = i / elapsed_s if elapsed_s > 0 else 1
            eta = (total - i) / rate / 60
            print(f"  ... 掃描進度 {i}/{total} ({pct:.0f}%)  ✅{ok_count} ⏭️{skip_count} ⚠️{err_count}  剩約{eta:.0f}分鐘", flush=True)

        # 檢查已有資料（missing-only 模式已在前置過濾，不需再逐支查詢）
        existing = item.get("existing_rows", 0)
        if not args.force and not args.missing_only:
            try:
                cache = get_cache_status(code)
                existing = cache.get("stock_daily_count", 0)
                if existing >= args.min_rows:
                    skip_count += 1
                    continue
            except Exception:
                pass

        # 執行回補
        try:
            t0 = time.time()
            result = run_on_demand_backfill(code, args.months, market_key)
            written = result.get("written_days", 0)
            errors  = result.get("errors", [])
            elapsed = time.time() - t0

            total_written += written
            chip_cnt = item.get("existing_chip", "-")
            if written > 0:
                ok_count += 1
                print(f"  {i:<5} {code:<6} {name:<10} {market_key:<5} {existing:>5} {chip_cnt!s:>5} {written:>5}筆  ✅ ({elapsed:.1f}s)", flush=True)
            else:
                err_count += 1
                errmsg = (errors[0][:38] if errors else "無資料或 API 無回應")
                print(f"  {i:<5} {code:<6} {name:<10} {market_key:<5} {existing:>5} {chip_cnt!s:>5} {'0':>5}筆  ⚠️  {errmsg}", flush=True)
        except Exception as e:
            err_count += 1
            chip_cnt = item.get("existing_chip", "-")
            print(f"  {i:<5} {code:<6} {name:<10} {market_key:<5} {existing:>5} {chip_cnt!s:>5} {'?':>5}筆  ❌ {str(e)[:38]}", flush=True)

        time.sleep(args.sleep)

        # 每 50 支印進度
        if i % 50 == 0:
            _print_progress(i, total, ok_count, skip_count, err_count, start_time)

    # ── 最終報告 ───────────────────────────────────────────
    elapsed = time.time() - start_time
    print("\n" + "=" * 65)
    print("  📊 完成報告")
    print(f"  耗時      : {elapsed/60:.1f} 分鐘")
    print(f"  總股票數  : {total}")
    print(f"  ✅ 成功寫入: {ok_count} 支（共 {total_written} 筆資料）")
    print(f"  ⏭️  略過    : {skip_count} 支（已有足夠資料）")
    print(f"  ⚠️  失敗    : {err_count} 支")
    print("=" * 65)
    if err_count > 0:
        print(f"\n  💡 失敗的股票可能是已下市、API 暫時無法存取，或為非一般股票")
        print(f"  💡 重新執行可補上暫時失敗的部分（已成功的會自動略過）")
    print()


def _print_progress(i, total, ok, skip, err, start_time):
    elapsed = time.time() - start_time
    rate = i / elapsed if elapsed > 0 else 1
    remaining = (total - i) / rate
    pct = i / total * 100
    print(f"\n  ── 進度 {i}/{total} ({pct:.0f}%)  ✅{ok} ⏭️{skip} ❌{err}  "
          f"剩餘約 {remaining/60:.0f} 分鐘 ──\n", flush=True)


if __name__ == "__main__":
    main()
