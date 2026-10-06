"""track_new.py — 把「新股票」納入每日追蹤清單並回補歷史。

新股票來源：
  1. 使用者交易紀錄（transactions）裡出現、但不在 backend/stocks.txt 的代號（持股自動納入）
  2. 前端「加入每日追蹤」按鈕寫入的 track_requests（status = pending）

對每檔新股票：
  - 寫入 product_universe（籌碼寫入的白名單）
  - 回補日K（run_on_demand_backfill，預設 24 個月）
  - 回補近 N 個交易日的法人／融資券籌碼（市場整批抓、只會寫入白名單股票）
  - 追加到 backend/stocks.txt（之後每日排程與 export 都會包含它）
最後印出新代號（逗號分隔）到 stdout 最後一行，供 workflow 接著 export --only。

Usage:
  python backend/track_new.py                    # 交易紀錄 + 待處理請求
  python backend/track_new.py --requests-only    # 只處理按鈕請求（track-stocks.yml）
  python backend/track_new.py --max 10 --months 24 --chip-days 40
"""
import argparse
import os
import sys

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BACKEND_DIR)
STOCKS_TXT = os.path.join(BACKEND_DIR, "stocks.txt")


def read_universe() -> list:
    with open(STOCKS_TXT, encoding="utf-8") as f:
        return [line.strip().upper() for line in f if line.strip()]


def append_universe(codes: list):
    current = read_universe()
    merged = sorted(set(current) | set(codes))
    with open(STOCKS_TXT, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(merged) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests-only", action="store_true")
    parser.add_argument("--max", type=int, default=10, help="每次最多處理幾檔（避免超時）")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--chip-days", type=int, default=40)
    args = parser.parse_args()

    import firebase_cache
    from firebase_cache import _run, save_product
    from jobs import run_chip_history_backfill, run_on_demand_backfill
    from stock_list import get_all_products

    universe = set(read_universe())

    candidates = {}
    rows, err = _run("SELECT code FROM track_requests WHERE status = 'pending'", fetch="all")
    if err:
        print(f"[track] track_requests 讀取失敗（migration 未套用？）: {err}")
    for (code,) in rows or []:
        candidates[str(code).upper()] = "request"
    if not args.requests_only:
        rows, err = _run("SELECT DISTINCT upper(stock_id) FROM transactions", fetch="all")
        if err:
            print(f"[track] transactions 讀取失敗: {err}")
        for (code,) in rows or []:
            candidates.setdefault(str(code).upper(), "holding")

    new = [c for c in sorted(candidates) if c not in universe]
    # 已在清單裡的請求直接標記完成
    for c in [c for c, src in candidates.items() if src == "request" and c in universe]:
        _run("UPDATE track_requests SET status='done', message='已在追蹤清單', done_at=now() WHERE code=%s", (c,))
    if not new:
        print("[track] 沒有新股票")
        print("")
        return 0

    products = {p["code"]: p for p in get_all_products()}
    todo = []
    for code in new[: args.max]:
        p = products.get(code)
        if not p:
            print(f"[track] {code} 查無此代號，略過")
            _run("UPDATE track_requests SET status='invalid', message='查無此代號', done_at=now() WHERE code=%s", (code,))
            continue
        save_product(code, p)
        todo.append((code, p))
    if len(new) > args.max:
        print(f"[track] 尚有 {len(new) - args.max} 檔留待下次處理")

    # 白名單快取重載，接下來的籌碼回補才會寫入新股票
    firebase_cache._CHIP_ALLOWED = None

    done = []
    for code, p in todo:
        print(f"[track] {code} {p.get('name')} ({p.get('market')}) 回補 {args.months} 個月日K…")
        try:
            r = run_on_demand_backfill(code, months=args.months, market=p.get("market") or "上市",
                                       product_type=p.get("type") or "股票")
            written = int(r.get("written_days") or 0)
            print(f"[track] {code} 寫入 {written} 天；errors={len(r.get('errors') or [])}")
            if written == 0:
                _run("UPDATE track_requests SET status='error', message='查無日K資料', done_at=now() WHERE code=%s", (code,))
                continue
            done.append(code)
        except Exception as exc:  # noqa: BLE001 — 單檔失敗不影響其他
            print(f"[track] {code} 回補失敗: {exc}")
            _run("UPDATE track_requests SET status='error', message=%s, done_at=now() WHERE code=%s", (str(exc)[:200], code))

    if done and args.chip_days > 0:
        print(f"[track] 回補近 {args.chip_days} 個交易日籌碼…")
        try:
            run_chip_history_backfill(max_days=args.chip_days, sleep_seconds=0.3)
        except Exception as exc:  # noqa: BLE001
            print(f"[track] 籌碼回補失敗（日K 已完成，下次每日排程會補當日籌碼）: {exc}")

    if done:
        append_universe(done)
        for code in done:
            _run("UPDATE track_requests SET status='done', message='已加入每日追蹤', done_at=now() WHERE code=%s", (code,))
    print(f"[track] 完成 {len(done)} 檔: {','.join(done)}")
    print(",".join(done))
    return 0


if __name__ == "__main__":
    sys.exit(main())
