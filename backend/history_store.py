"""history_store.py — Parquet 歷史資料倉（stock_daily / chip_daily 的完整副本）。

Supabase 只保留近 13 個月供網站使用；完整歷史存放在 GitHub Release `history-data`
的 Parquet 檔（依年份分檔）。策略：先複製、驗證，再刪除（prune 前一定先 verify）。

  history/prices_YYYY.parquet   stock_daily（不含 updated_at）
  history/chips_YYYY.parquet    chip_daily（不含 updated_at）
  history/manifest.json         每檔 rows / min,max date / ids / sha256 + 非交易日清單

子指令：
  pull | push | export-db [--since] | backfill | append --days N | verify --before
  prune --keep-months 13 [--apply] [--vacuum-full] | stats | query "SQL"

Usage 範例：
  python backend/history_store.py export-db
  python backend/history_store.py backfill --from-year 2025 --to-year 2025 --dry-run
  python backend/history_store.py prune --keep-months 13
"""
import argparse
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BACKEND_DIR)
HISTORY_DIR = os.environ.get("HISTORY_DIR", os.path.join(REPO_ROOT, "history"))
RELEASE_TAG = "history-data"
MANIFEST = "manifest.json"
PULLED = ".pulled.json"  # pull 當下各檔 sha256，push 只上傳有變動的檔

FLOAT_P = ["open", "high", "low", "close", "volume", "turnover", "change", "trades"]
PRICE_COLS = ["stock_id", "date"] + FLOAT_P + ["market", "product_type", "name", "source"]
INT_C = ["foreign_buy", "investment_trust_buy", "dealer_buy", "institution_total_buy",
         "margin_balance", "short_balance"]
CHIP_COLS = ["stock_id", "date", "name", "market"] + INT_C + ["source", "chip_date"]
KINDS = {
    "prices": {"table": "stock_daily", "cols": PRICE_COLS, "float": FLOAT_P, "int": []},
    "chips": {"table": "chip_daily", "cols": CHIP_COLS, "float": [], "int": INT_C},
}
# backfill 判定「該日已完整」的最低筆數（兩市場合計），避免只抓到單一市場被當成完成
MIN_ROWS = {"prices": 1500, "chips": 700}


TW_TZ = timezone(timedelta(hours=8))  # 台灣無日光節約時間，固定 UTC+8（免 tzdata 依賴）


def today_tw():
    return datetime.now(TW_TZ).date()


class HistoryError(RuntimeError):
    """資料倉流程中的致命錯誤（一律中止，不做任何刪除/覆蓋）。"""


def log(msg):
    print(msg, flush=True)


# ── 基礎：檔案讀寫 ─────────────────────────────────────────────────────────────
def load_env():
    from export_static_json import load_env_file
    load_env_file()


def year_path(kind, year):
    return os.path.join(HISTORY_DIR, f"{kind}_{year}.parquet")


def year_files(kind, store_dir=None):
    return sorted(glob.glob(os.path.join(store_dir or HISTORY_DIR, f"{kind}_*.parquet")))


def normalize(df, kind):
    spec = KINDS[kind]
    df = df.reindex(columns=spec["cols"]).copy()
    for c in spec["float"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in spec["int"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in spec["cols"]:
        if c not in spec["float"] and c not in spec["int"]:
            df[c] = df[c].astype("object").where(df[c].notna(), None)
    return df


def schema_for(kind):
    spec = KINDS[kind]
    fields = []
    for c in spec["cols"]:
        if c in spec["float"]:
            fields.append(pa.field(c, pa.float64()))
        elif c in spec["int"]:
            fields.append(pa.field(c, pa.int64()))
        else:
            fields.append(pa.field(c, pa.string()))
    return pa.schema(fields)


def read_year(kind, year):
    p = year_path(kind, year)
    if not os.path.exists(p):
        return None
    return normalize(pq.read_table(p).to_pandas(), kind)


def read_all(kind, columns=None, store_dir=None):
    frames = [pq.read_table(p, columns=columns).to_pandas() for p in year_files(kind, store_dir)]
    if not frames:
        return pd.DataFrame(columns=columns or KINDS[kind]["cols"])
    return pd.concat(frames, ignore_index=True)


def write_year(kind, year, df):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=schema_for(kind), preserve_index=False)
    tmp = year_path(kind, year) + ".tmp"
    pq.write_table(table, tmp, compression="zstd", compression_level=12, row_group_size=200_000)
    os.replace(tmp, year_path(kind, year))


def merge_into_store(kind, df, prefer="new"):
    """依年份合併，內容沒變就不重寫檔案。回傳變動的年份。
    prefer="new"：同 (stock_id,date) 以新資料為準（DB 匯出/append：DB 是近期資料的真實來源）。
    prefer="old"：既有非空值絕不覆蓋；缺少的 key 新增，既有列的 NULL 欄位由新資料補上（backfill）。"""
    if df is None or df.empty:
        return []
    df = normalize(df, kind)
    changed = []
    for year, part in df.groupby(df["date"].str[:4]):
        old = read_year(kind, year)
        if old is None:
            combined = part.drop_duplicates(["stock_id", "date"], keep="last")
        elif prefer == "old":
            # 既有值優先；既有為 NULL 的欄位由新資料補上（groupby.first 取每欄第一個非空值）
            combined = normalize(pd.concat([old, part], ignore_index=True)
                                 .groupby(["stock_id", "date"], sort=False, as_index=False).first(), kind)
        else:
            combined = pd.concat([old, part], ignore_index=True).drop_duplicates(["stock_id", "date"], keep="last")
        combined = combined.sort_values(["stock_id", "date"]).reset_index(drop=True)
        if old is not None:
            old = old.sort_values(["stock_id", "date"]).reset_index(drop=True)
            if len(old) == len(combined) and old.equals(combined):
                continue
        write_year(kind, year, combined)
        changed.append(year)
        log(f"  [store] {kind}_{year}.parquet rows={len(combined):,}")
    return changed


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def file_summary(path):
    t = pq.read_table(path, columns=["stock_id", "date"]).to_pandas()
    return {
        "rows": int(len(t)), "min_date": t["date"].min(), "max_date": t["date"].max(),
        "ids": int(t["stock_id"].nunique()), "size": os.path.getsize(path),
        "sha256": sha256_of(path),
        "updated_at": datetime.fromtimestamp(os.path.getmtime(path), timezone.utc).isoformat(timespec="seconds"),
    }


def gh(*args, check=True):
    return subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8", check=check)


def gh_retry(*args, tries=3):
    """gh 指令失敗時重試（5s、15s 退避）；仍失敗就丟 HistoryError。"""
    err = ""
    for i in range(tries):
        r = gh(*args, check=False)
        if r.returncode == 0:
            return r
        err = (r.stderr or "").strip()[:200]
        if i < tries - 1:
            time.sleep(5 * 3 ** i)
    raise HistoryError(f"gh {' '.join(args[:2])} 重試 {tries} 次仍失敗：{err}")


def release_assets():
    """回傳 release 的 asset 名稱清單；release 不存在回傳 None；其他任何錯誤一律丟 HistoryError。"""
    r = gh("release", "view", RELEASE_TAG, "--json", "assets", check=False)
    if r.returncode == 0:
        return [a["name"] for a in json.loads(r.stdout).get("assets", [])]
    err = (r.stderr or "").lower()
    if "release not found" in err or "http 404" in err:
        return None
    raise HistoryError(f"gh release view 失敗（非 not-found）：{(r.stderr or '').strip()[:200]}")


def _download(dest, pattern):
    r = gh("release", "download", RELEASE_TAG, "--dir", dest, "--clobber", "--pattern", pattern, check=False)
    if r.returncode != 0:
        raise HistoryError(f"下載 {pattern} 失敗：{(r.stderr or '').strip()[:200]}")


def fetch_remote_manifest(dest, lenient=False):
    """只下載 manifest。回傳 (manifest, sha256)；release 不存在回傳 (None, None)；
    release 存在但沒有 manifest（空 release）回傳 ({"files": {}}, None)。"""
    names = release_assets()
    if names is None:
        return None, None
    if MANIFEST not in names:
        if any(n.endswith(".parquet") for n in names) and not lenient:
            raise HistoryError("release 有 parquet 卻沒有 manifest.json，拒絕繼續（可用 pull --repair）")
        return {"files": {}, "nontrading_dates": []}, None
    _download(dest, MANIFEST)
    path = os.path.join(dest, MANIFEST)
    manifest = read_json(path, None)
    if not isinstance(manifest, dict) or "files" not in manifest:
        if lenient:
            return {"files": {}, "nontrading_dates": []}, sha256_of(path)
        raise HistoryError("remote manifest.json 無法解析")
    return manifest, sha256_of(path)


def download_release(dest):
    """把 release 全部資料檔下載到 dest，逐檔驗證 sha256 與 Parquet 可開啟、列數相符，任何不符即失敗。
    回傳 (manifest, manifest_sha)；release 不存在回傳 (None, None)。"""
    os.makedirs(dest, exist_ok=True)
    manifest, msha = fetch_remote_manifest(dest)
    if manifest is None:
        return None, None
    for name, info in manifest["files"].items():
        _download(dest, name)
        path = os.path.join(dest, name)
        if not os.path.exists(path):
            raise HistoryError(f"{name} 下載後不存在")
        if sha256_of(path) != info["sha256"]:
            raise HistoryError(f"{name} sha256 與 manifest 不符")
        if pq.ParquetFile(path).metadata.num_rows != info["rows"]:
            raise HistoryError(f"{name} 列數與 manifest 不符")
    return manifest, msha


def repair_release(dest, allow_shrink=False):
    """push 中途失敗（資料檔已換、manifest 沒換）後的修復：下載實際 assets，驗證可開啟，
    依實際檔案重建 manifest。assets 缺漏/損壞，或重建列數少於舊 manifest（疑似資料遺失）一律失敗。
    回傳 (重建 manifest, 遠端現有 manifest sha)。"""
    names = release_assets()
    if names is None:
        raise HistoryError("release 不存在，無需修復")
    old, osha = fetch_remote_manifest(dest, lenient=True)
    data_assets = sorted(n for n in names if n.endswith(".parquet"))
    for n in old["files"]:
        if n not in data_assets:
            raise HistoryError(f"舊 manifest 列出的 {n} 在 release 中不存在，疑似遺失，拒絕修復")
    files = {}
    for n in data_assets:
        _download(dest, n)
        path = os.path.join(dest, n)
        try:
            rows = pq.ParquetFile(path).metadata.num_rows
            files[n] = file_summary(path)
        except Exception as exc:
            raise HistoryError(f"{n} 無法開啟：{str(exc)[:100]}")
        was = (old["files"].get(n) or {}).get("rows", 0)
        if rows < was and not allow_shrink:
            raise HistoryError(f"{n} 實際列數 {rows} < 舊 manifest {was}，疑似資料遺失（需 --allow-shrink）")
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "files": files, "nontrading_dates": old.get("nontrading_dates", []),
                "tpex_chips_fixed": old.get("tpex_chips_fixed", [])}
    with open(os.path.join(dest, MANIFEST), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    return manifest, osha


def cmd_pull(args):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    if getattr(args, "repair", False):
        manifest, osha = repair_release(HISTORY_DIR, getattr(args, "allow_shrink", False))
        state = {"release_missing": False, "manifest_sha": osha, "repair": True,
                 "files": {n: i["sha256"] for n, i in manifest["files"].items()}}
        with open(os.path.join(HISTORY_DIR, PULLED), "w", encoding="utf-8") as f:
            json.dump(state, f)
        log(f"[pull --repair] 已依實際 {len(manifest['files'])} 個檔案重建 manifest；請執行 push 上傳修復後的 manifest")
        return 0
    manifest, msha = download_release(HISTORY_DIR)
    if manifest is None:
        log(f"[pull] release {RELEASE_TAG} 不存在，視為首次建立")
        state = {"release_missing": True, "manifest_sha": None, "files": {}}
    else:
        state = {"release_missing": False, "manifest_sha": msha,
                 "files": {n: i["sha256"] for n, i in manifest["files"].items()}}
        log(f"[pull] 下載並驗證 {len(state['files'])} 個檔案")
    with open(os.path.join(HISTORY_DIR, PULLED), "w", encoding="utf-8") as f:
        json.dump(state, f)
    return 0


def plan_push(remote, pulled, local_files, allow_shrink):
    """純邏輯：回傳要上傳的檔名清單；任何風險丟 HistoryError。
    remote: 遠端 manifest 或 None；pulled: pull 當下記錄；local_files: {name: summary}。"""
    if remote is None and not pulled.get("release_missing"):
        raise HistoryError("pull 時 release 存在，現在卻不存在，拒絕 push")
    todo = []
    for name, s in local_files.items():
        if pulled.get("files", {}).get(name) == s["sha256"]:
            continue
        old_rows = ((remote or {}).get("files", {}).get(name) or {}).get("rows", 0)
        if s["rows"] < old_rows and not allow_shrink:
            raise HistoryError(f"{name} 列數 {s['rows']} < 遠端 {old_rows}，拒絕覆蓋（需 --allow-shrink）")
        todo.append(name)
    return todo


def cmd_push(args):
    pulled = read_json(os.path.join(HISTORY_DIR, PULLED), None)
    if pulled is None:
        raise HistoryError("找不到 .pulled.json：push 前必須先 pull，拒絕上傳")
    local = {os.path.basename(p): file_summary(p) for k in KINDS for p in year_files(k)}
    if not local:
        log("[push] history/ 沒有 parquet，略過")
        return 0
    repair = bool(pulled.get("repair"))
    with tempfile.TemporaryDirectory(prefix="hist_chk_") as tmp:
        remote, rsha = fetch_remote_manifest(tmp, lenient=repair)
    if remote is not None and rsha != pulled.get("manifest_sha"):
        raise HistoryError("遠端 manifest 自 pull 後已變動（可能有並行寫入），請重新 pull 再試")
    todo = plan_push(remote, pulled, local, args.allow_shrink)
    if not todo and not repair:
        log("[push] 沒有變動的檔案")
        return 0
    mpath = os.path.join(HISTORY_DIR, MANIFEST)
    nontrading = sorted(set(read_json(mpath, {}).get("nontrading_dates", []))
                        | set((remote or {}).get("nontrading_dates", [])))
    files = {**((remote or {}).get("files", {})), **local}  # 遠端有、本地沒有的檔，manifest 照舊保留
    if repair:
        files = dict(local)  # 修復模式：以實際檔案為準（pull --repair 已確認無缺漏）
    tpex_fixed = sorted(set(read_json(mpath, {}).get("tpex_chips_fixed", []))
                        | set((remote or {}).get("tpex_chips_fixed", [])))
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "files": files, "nontrading_dates": nontrading, "tpex_chips_fixed": tpex_fixed}
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    if remote is None:
        gh("release", "create", RELEASE_TAG, "--title", "History data (parquet)",
           "--notes", "自動產生的市場歷史資料（stock_daily / chip_daily 完整副本，依年份分檔 Parquet）。"
                      "由 backend/history_store.py 與 GitHub Actions 維護，請勿手動編輯。")
        log("[push] 已建立 release " + RELEASE_TAG)
    for name in todo:
        log(f"[push] upload {name}")
        gh_retry("release", "upload", RELEASE_TAG, os.path.join(HISTORY_DIR, name), "--clobber")
    gh_retry("release", "upload", RELEASE_TAG, mpath, "--clobber")  # manifest 最後上傳
    # 上傳後重新下載驗證（sha256 / 列數）
    with tempfile.TemporaryDirectory(prefix="hist_post_") as tmp:
        m2, msha2 = download_release(tmp)
        if m2 is None or msha2 != sha256_of(mpath):
            raise HistoryError("上傳後驗證失敗：remote manifest 與本地不一致")
        for name in todo:
            if sha256_of(os.path.join(tmp, name)) != local[name]["sha256"]:
                raise HistoryError(f"上傳後驗證失敗：{name}")
    with open(os.path.join(HISTORY_DIR, PULLED), "w", encoding="utf-8") as f:
        json.dump({"release_missing": False, "manifest_sha": sha256_of(mpath),
                   "files": {n: i["sha256"] for n, i in files.items()}}, f)
    log(f"[push] 完成並驗證 {len(todo)} 個檔案" + ("（含修復 manifest）" if repair else ""))
    return 0


# ── DB（唯讀）─────────────────────────────────────────────────────────────────
def session_url(url):
    """prune 需要 session 級連線（LOCK / VACUUM）：優先用 DIRECT_DATABASE_URL；
    若是 transaction pooler（:6543）則改成同主機 session mode（:5432）。"""
    return url.replace(":6543/", ":5432/")


def connect(readonly=True, session=False):
    import psycopg2
    load_env()
    url = os.environ.get("DATABASE_URL")
    if session:
        url = os.environ.get("DIRECT_DATABASE_URL") or url
    if not url:
        raise SystemExit("DATABASE_URL not set")
    conn = psycopg2.connect(session_url(url) if session else url)
    if readonly:
        conn.set_session(readonly=True)
    with conn.cursor() as cur:  # 同一 session 內設定 timeout
        cur.execute("SET statement_timeout = '30min'")
        cur.execute("SET lock_timeout = '60s'")
    conn.commit()  # 結束 SET 開啟的交易，之後才能切 autocommit
    return conn


def stream_table(conn, kind, where="", params=(), chunk=100_000, columns=None):
    """server-side named cursor 分批讀取，yield DataFrame。"""
    cols = columns or KINDS[kind]["cols"]
    sql = f"SELECT {','.join(cols)} FROM {KINDS[kind]['table']} {where}"
    with conn.cursor(name=f"hist_{kind}_{int(time.time() * 1000)}") as cur:
        cur.itersize = chunk
        cur.execute(sql, params)
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=cols)


def export_db(since=None):
    conn = connect(readonly=True)
    try:
        for kind in KINDS:
            where, params = ("WHERE date >= %s", (since,)) if since else ("", ())
            frames, n = [], 0
            for df in stream_table(conn, kind, where, params):
                frames.append(normalize(drop_warrants(df), kind))
                n += len(df)
                log(f"  [export] {kind}: {n:,} rows")
            if frames:
                merge_into_store(kind, pd.concat(frames, ignore_index=True), prefer="new")  # DB 為近期資料真實來源
            log(f"[export-db] {kind}: {n:,} rows merged")
    finally:
        conn.rollback()
        conn.close()


def cmd_export_db(args):
    export_db(args.since)
    return 0


def cmd_append(args):
    export_db((today_tw() - timedelta(days=args.days)).strftime("%Y%m%d"))
    return 0


# ── backfill（只寫 Parquet，絕不寫 Supabase）──────────────────────────────────
from instrument_filter import is_warrant  # 權證判斷唯一來源（backend/instrument_filter.py）


def drop_warrants(df):
    return df[~df["stock_id"].map(is_warrant)] if len(df) else df


def fetch_prices_day(jobs, d):
    """TWSE MI_INDEX（ALL）+ TPEx otc（AL）單日全市場。回傳 (rows, status)；
    status: ok | closed（休市/無資料）| error。"""
    rows = []
    payload, err = jobs.fetch_json(
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
        params={"date": d, "type": "ALL", "response": "json"})
    if err and not payload:
        return rows, "error"
    table = None
    if isinstance(payload, dict) and payload.get("stat") == "OK" and str(payload.get("date")) == d:
        for t in payload.get("tables") or []:
            if "每日收盤行情" in str(t.get("title", "")) and t.get("data"):
                table = (t["fields"], t["data"])
                break
        if table is None and payload.get("data9"):  # 舊版格式
            table = (payload.get("fields9") or [], payload["data9"])
    if not table:
        # 只有 TWSE 明確回「沒有符合條件的資料」才算休市；節流/格式異常/日期不符一律視為 error
        no_data = isinstance(payload, dict) and not err and "沒有符合條件" in str(payload.get("stat", ""))
        return [], ("closed" if no_data else "error")
    fields, data = table
    ix = {k: jobs._idx(fields, k) for k in
          ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(", "漲跌價差"]}
    for r in data:
        sid = str(r[ix["證券代號"]]).strip()
        close = jobs.safe_float(r[ix["收盤價"]])
        if not sid or is_warrant(sid) or close is None or close <= 0:
            continue
        sign = re.sub(r"<[^>]+>", "", str(r[ix["漲跌("]])).strip() if ix["漲跌("] is not None else ""
        diff = jobs.safe_float(r[ix["漲跌價差"]])
        if diff is not None and sign == "-":
            diff = -diff
        elif sign == "X":
            diff = None
        rows.append({
            "stock_id": sid, "date": d, "open": jobs.safe_float(r[ix["開盤價"]]) or close,
            "high": jobs.safe_float(r[ix["最高價"]]) or close, "low": jobs.safe_float(r[ix["最低價"]]) or close,
            "close": close, "volume": jobs.safe_float(r[ix["成交股數"]]),
            "turnover": jobs.safe_float(r[ix["成交金額"]]), "change": diff,
            "trades": jobs.safe_float(r[ix["成交筆數"]]), "market": "TWSE",
            "product_type": None, "name": r[ix["證券名稱"]], "source": "TWSE MI_INDEX"})
    p2, err2 = jobs.fetch_json(
        "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc",
        params={"date": f"{d[:4]}/{d[4:6]}/{d[6:]}", "type": "AL", "response": "json"},
        headers=jobs.TPEX_HEADERS)
    t0 = (p2.get("tables") or [{}])[0] if isinstance(p2, dict) else {}
    if t0.get("data") and jobs._roc_any_to_yyyymmdd(t0.get("date") or "") == d:
        f2 = [str(x).strip() for x in t0.get("fields") or []]
        c = {k: jobs._idx(f2, k) for k in ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數", "成交金額", "成交筆數"]}
        for r in t0["data"]:
            sid = str(r[c["代號"]]).strip()
            close = jobs.safe_float(r[c["收盤"]])
            if not sid or is_warrant(sid) or close is None or close <= 0:
                continue
            rows.append({
                "stock_id": sid, "date": d, "open": jobs.safe_float(r[c["開盤"]]) or close,
                "high": jobs.safe_float(r[c["最高"]]) or close, "low": jobs.safe_float(r[c["最低"]]) or close,
                "close": close, "volume": jobs.safe_float(r[c["成交股數"]]),
                "turnover": jobs.safe_float(r[c["成交金額"]]), "change": jobs.safe_float(r[c["漲跌"]]),
                "trades": jobs.safe_float(r[c["成交筆數"]]), "market": "TPEx",
                "product_type": None, "name": r[c["名稱"]], "source": "TPEx otc"})
    else:  # TWSE 有資料但 TPEx 失敗：整日視為 error，不合併殘缺資料
        log(f"    TPEx prices {d} 失敗或日期不符：{str(err2)[:100]}")
        return [], "error"
    return rows, "ok"


def fetch_chips_day(jobs, d):
    """重用 jobs 的 T86 / MI_MARGN / TPEx insti / TPEx margin 解析，把寫 DB 改成存記憶體。"""
    captured = []
    jobs.save_chip_daily_bulk = lambda rows: (captured.extend(rows), len(rows))[1]
    jobs.back_dates_from = lambda base, n=10: iter([base])  # 只抓指定日，不往回找
    res = {"errors": [], "chips": 0}
    jobs.write_t86_chips(d, res)
    # jobs.write_margin_chips 取 tables[0]（統計摘要表），TWSE 融資券實際在「融資融券彙總」表，這裡自行解析
    mp, merr = jobs.fetch_json(jobs.TWSE_MARGIN, params={"response": "json", "date": d, "selectType": "ALL"})
    if merr and not mp:
        res["errors"].append(f"MI_MARGN {d}: {merr}")
    for t in (mp.get("tables") or []) if isinstance(mp, dict) else []:
        if "彙總" in str(t.get("title", "")) and t.get("data"):
            for r in t["data"]:
                sid = str(r[0]).strip()
                if sid and len(r) > 12:
                    captured.append((sid, d, {"market": "TWSE", "name": r[1],
                                              "margin_balance": jobs.safe_int(r[6]),
                                              "short_balance": jobs.safe_int(r[12]),
                                              "source": "TWSE MI_MARGN"}))
    jobs.write_tpex_insti_chips(d, res)
    jobs.write_tpex_margin_chips(d, res)
    for e in res["errors"][:3]:
        log(f"    chips {d}: {str(e)[:120]}")
    return merge_chip_docs(captured, d)


def merge_chip_docs(captured, d):
    """jobs 各 writer 擷取到的 (sid, date, doc) 依 stock_id 合併成 store 列。"""
    merged = {}
    for sid, dt, doc in captured:
        if dt != d or is_warrant(sid):
            continue
        cur = merged.setdefault(sid, {"stock_id": sid, "date": d})
        for k in ["name", "market", "foreign_buy", "investment_trust_buy", "dealer_buy", "institution_total_buy"]:
            if doc.get(k) is not None:
                cur[k] = doc[k]
        mb = doc.get("margin_balance") if doc.get("margin_balance") is not None else doc.get("margin")
        sb = doc.get("short_balance") if doc.get("short_balance") is not None else doc.get("short")
        if mb is not None:
            cur["margin_balance"] = mb
        if sb is not None:
            cur["short_balance"] = sb
        cur["source"] = doc.get("source")
        cur["chip_date"] = doc.get("chip_date") or d
    return list(merged.values())


def weekdays_desc(start, end):
    """start~end 之間的平日，新 → 舊。"""
    d = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    while e >= d:
        if e.weekday() < 5:
            yield e.strftime("%Y%m%d")
        e -= timedelta(days=1)


def cmd_backfill(args):
    load_env()
    _db_url = os.environ.get("DATABASE_URL")
    # 重要：backfill 絕不寫 Supabase — 先移除 DATABASE_URL，jobs/firebase 就不會建立 DB 連線
    # （上面保留 _db_url 只用於稍後一次唯讀查詢已有資料的日期）
    os.environ.pop("DATABASE_URL", None)
    sys.path.insert(0, BACKEND_DIR)
    import jobs

    real_fetch = jobs.fetch_json

    def polite_fetch(url, params=None, headers=None, retries=2):
        err = None
        for attempt in range(4):
            time.sleep(args.sleep)
            payload, err = real_fetch(url, params=params, headers=headers, retries=retries)
            if not err or payload:
                return payload, err
            wait = 5 * 3 ** attempt
            log(f"    retry in {wait}s ({str(err)[:80]})")
            time.sleep(wait)
        return {}, err
    jobs.fetch_json = polite_fetch

    today = today_tw()
    yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
    start = args.start or today.replace(year=today.year - args.years).strftime("%Y%m%d")
    end = args.end or yesterday
    if args.from_year:
        start = f"{args.from_year}0101"
    if args.to_year:
        end = f"{args.to_year}1231"
    end = min(end, yesterday)
    kinds = ["prices", "chips"] if args.kind == "both" else [args.kind]

    mpath = os.path.join(HISTORY_DIR, MANIFEST)
    nontrading = set(read_json(mpath, {}).get("nontrading_dates", []))
    db_dates = set()  # Supabase 已有資料的日期（有資料就絕不標為休市）；僅唯讀查詢，之後移除 DATABASE_URL
    if _db_url:
        try:
            os.environ["DATABASE_URL"] = _db_url
            c = connect(readonly=True)
            with c.cursor() as cur:
                cur.execute("SELECT DISTINCT date FROM stock_daily WHERE date BETWEEN %s AND %s", (start, end))
                db_dates = {r[0] for r in cur.fetchall()}
            c.rollback()
            c.close()
        except Exception as exc:
            log(f"[backfill] 無法查 DB 日期（略過，僅以 store 判斷）：{str(exc)[:80]}")
        finally:
            os.environ.pop("DATABASE_URL", None)
    counts = {}
    for k in kinds:
        df = read_all(k, columns=["date"])
        counts[k] = df["date"].value_counts().to_dict() if len(df) else {}
    todo = []
    for d in weekdays_desc(start, end):
        if d in nontrading:
            continue
        need = [k for k in kinds if counts[k].get(d, 0) < MIN_ROWS[k]]
        if need:
            todo.append((d, need))
    if args.limit_dates:
        todo = todo[: args.limit_dates]
    req_per = {"prices": 2, "chips": 4}
    reqs = sum(req_per[k] for _, need in todo for k in need)
    log(f"[backfill] 範圍 {start}~{end} kind={args.kind} 待處理 {len(todo)} 日, 約 {reqs} 次請求, "
        f"預估 {reqs * (args.sleep + 0.7) / 60:.0f} 分鐘")
    if args.dry_run:
        for d, need in todo[:10]:
            log(f"  - {d} {'+'.join(need)}")
        if len(todo) > 10:
            log(f"  ... 另 {len(todo) - 10} 日")
        return 0

    buf = {"prices": [], "chips": []}

    def flush():
        for k in kinds:
            if buf[k]:
                merge_into_store(k, pd.DataFrame(buf[k]), prefer="old")
                buf[k] = []
        m = read_json(mpath, {})
        m["nontrading_dates"] = sorted(nontrading)
        os.makedirs(HISTORY_DIR, exist_ok=True)
        with open(mpath, "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=1)

    fails = 0
    for i, (d, need) in enumerate(todo, 1):
        closed = False
        if "prices" in need:
            rows, status = fetch_prices_day(jobs, d)
            if status == "error":
                fails += 1
                log(f"  [{i}/{len(todo)}] {d} prices 取得失敗 (連續 {fails})")
                if fails >= 5:
                    log("[backfill] 連續失敗 5 次，疑似被封鎖或網路不通，中止")
                    flush()
                    return 2
                continue
            fails = 0
            if status == "closed":
                closed = True
                if d in db_dates or any(counts[k].get(d, 0) for k in counts):
                    log(f"  [{i}/{len(todo)}] {d} TWSE 回無資料但 DB/store 已有該日資料，不標休市")
                else:
                    nontrading.add(d)
                    log(f"  [{i}/{len(todo)}] {d} 休市")
            else:
                buf["prices"].extend(rows)
                log(f"  [{i}/{len(todo)}] {d} prices rows={len(rows)}")
        if "chips" in need and not closed:
            crow = fetch_chips_day(jobs, d)
            buf["chips"].extend(crow)
            log(f"  [{i}/{len(todo)}] {d} chips rows={len(crow)}")
        if i % 20 == 0:
            flush()
    flush()
    log("[backfill] 完成")
    return 0


def cmd_fix_tpex_chips(args):
    """重抓上櫃籌碼覆蓋 store 中的 TPEx 列（舊版傳 YYYYMMDD 給 TPEx 被忽略、拿到最新日資料；
    融資取到前日餘額；三大法人合計恆為 NULL）。只動 market=TPEx 的列，TWSE 列不變。
    已修好的日期記在 manifest tpex_chips_fixed，可分次 dispatch 續跑。
    前提：Supabase 必須先用 repair_tpex_chips.py --apply 修好，否則之後的 export-db（prefer=new）
    會把 DB 仍錯誤的近期 TPEx 列蓋回 store。"""
    load_env()
    os.environ.pop("DATABASE_URL", None)  # 只寫 Parquet，絕不碰 DB
    sys.path.insert(0, BACKEND_DIR)
    import jobs

    mpath = os.path.join(HISTORY_DIR, MANIFEST)
    fixed = set(read_json(mpath, {}).get("tpex_chips_fixed", []))
    prices = read_all("prices", columns=["date", "market"])
    lo, hi = f"{args.from_year or 1900}0101", f"{args.to_year or 2999}1231"
    dates = sorted({d for d in prices.loc[prices["market"] == "TPEx", "date"].unique()
                    if lo <= d <= hi and d not in fixed}, reverse=True)
    if args.limit_dates:
        dates = dates[: args.limit_dates]
    log(f"[fix-tpex] 待處理 {len(dates)} 日（已完成 {len(fixed)}）")
    if args.dry_run:
        return 0

    chips = read_all("chips", columns=["date", "market"])
    old_tpex = chips.loc[chips["market"] == "TPEx", "date"].value_counts().to_dict() if len(chips) else {}
    del chips, prices
    captured = []
    jobs.save_chip_daily_bulk = lambda rows: (captured.extend(rows), len(rows))[1]
    buf = {}

    def flush():
        if not buf:
            return
        by_year = {}
        for d, rows in buf.items():
            by_year.setdefault(d[:4], {})[d] = rows
        for year, days in by_year.items():
            old = read_year("chips", year)
            new = normalize(pd.DataFrame([r for rows in days.values() for r in rows]), "chips")
            if old is not None:
                drop = old["date"].isin(list(days)) & (old["market"] == "TPEx")
                # 同日同股若已有 TWSE 列（不應發生），保留 TWSE、捨棄新 TPEx 列
                keep_old = old[~drop]
                twse_keys = set(zip(keep_old.loc[keep_old["date"].isin(list(days)), "stock_id"],
                                    keep_old.loc[keep_old["date"].isin(list(days)), "date"]))
                new = new[[k not in twse_keys for k in zip(new["stock_id"], new["date"])]]
                combined = pd.concat([keep_old, new], ignore_index=True)
            else:
                combined = new
            combined = combined.sort_values(["stock_id", "date"]).reset_index(drop=True)
            write_year("chips", year, combined)
            log(f"  [store] chips_{year}.parquet rows={len(combined):,}")
        fixed.update(buf)
        m = read_json(mpath, {})
        m["tpex_chips_fixed"] = sorted(fixed)
        with open(mpath + ".tmp", "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=1)
        os.replace(mpath + ".tmp", mpath)
        buf.clear()

    fails = 0
    for i, d in enumerate(dates, 1):
        captured.clear()
        res = {"errors": []}
        time.sleep(args.sleep)
        n1 = jobs.write_tpex_insti_chips(d, res)
        time.sleep(args.sleep)
        n2 = jobs.write_tpex_margin_chips(d, res)
        if any("server returned data for" in e for e in res["errors"]):
            log(f"  [{i}/{len(dates)}] {d} TPEx 回其他日期資料（休市？），略過不改")
            continue
        if not n1 and not n2 and not res["errors"]:
            log(f"  [{i}/{len(dates)}] {d} TPEx 無資料（休市），略過不改")
            continue
        if not n1 or not n2:
            fails += 1
            log(f"  [{i}/{len(dates)}] {d} 失敗 (連續 {fails}): {'; '.join(res['errors'][:2])[:150]}")
            if fails >= 5:
                log("[fix-tpex] 連續失敗 5 次，中止")
                flush()
                return 2
            continue
        fails = 0
        rows = [r for r in merge_chip_docs(captured, d) if r.get("market") == "TPEx"]
        floor = max(50, int(old_tpex.get(d, 0) * 0.5))
        if len(rows) < floor:
            fails += 1
            log(f"  [{i}/{len(dates)}] {d} TPEx rows={len(rows)} < {floor}（舊 {old_tpex.get(d, 0)}），不覆蓋 (連續 {fails})")
            if fails >= 5:
                log("[fix-tpex] 連續失敗 5 次，中止")
                flush()
                return 2
            continue
        for r in rows:
            r["source"] = "TPEx insti/dailyTrade+margin/balance"
        buf[d] = rows
        log(f"  [{i}/{len(dates)}] {d} TPEx rows={len(rows)}")
        if len(buf) >= 20:
            flush()
    flush()
    log("[fix-tpex] 完成")
    return 0


# ── verify / prune ────────────────────────────────────────────────────────────
def verify(cutoff, conn=None, store_dir=None):
    """Supabase 中 date < cutoff 的每一筆 (stock_id,date) 都必須存在於 store（store_dir 預設本機 history/）。"""
    own = conn is None
    conn = conn or connect(readonly=True)
    ok = True
    try:
        for kind, spec in KINDS.items():
            t = spec["table"]
            store = read_all(kind, columns=["stock_id", "date"], store_dir=store_dir)
            store = store[store["date"] < cutoff]
            keys = pd.MultiIndex.from_frame(store[["stock_id", "date"]]) if len(store) else None
            db_n = missing = 0
            sample = []
            dbc = {}
            for df in stream_table(conn, kind, "WHERE date < %s", (cutoff,), columns=["stock_id", "date"]):
                df = drop_warrants(df)  # 權證不進歷史資料倉，DB 裡的權證不算缺漏
                for d_, n_ in df["date"].value_counts().items():
                    dbc[d_] = dbc.get(d_, 0) + n_
                db_n += len(df)
                miss = df if keys is None else df[~pd.MultiIndex.from_frame(df).isin(keys)]
                missing += len(miss)
                if len(sample) < 5 and len(miss):
                    sample += miss.head(5).values.tolist()
            sc = store["date"].value_counts().to_dict()
            short = {d: (n, sc.get(d, 0)) for d, n in dbc.items() if sc.get(d, 0) < n}
            log(f"[verify] {t}: DB rows(<{cutoff})={db_n:,} store rows={len(store):,} "
                f"missing_in_store={missing:,} dates_short={len(short)}")
            if missing or short:
                ok = False
                log(f"  FAIL sample missing={sample} short(date:(db,store))={list(short.items())[:5]}")
    finally:
        if own:
            conn.rollback()
            conn.close()
    return ok


def cmd_verify(args):
    return 0 if verify(args.before, store_dir=args.store_dir) else 1


def months_cutoff(keep_months, today=None):
    t = today or today_tw()
    m = t.year * 12 + (t.month - 1) - keep_months
    return f"{m // 12:04d}{m % 12 + 1:02d}01"


def db_size_mb(cur):
    cur.execute("SELECT pg_database_size(current_database())")
    return cur.fetchone()[0] / 1048576


def select_deletable(db_ids, store_ids):
    """只有「DB 有、且已驗證存在於 store」的 key 才能刪；store 沒有的一律保留。"""
    keep = set(store_ids)
    return sorted(i for i in set(db_ids) if i in keep)


def store_keys_by_date(kind, store_dir, cutoff):
    df = read_all(kind, columns=["stock_id", "date"], store_dir=store_dir)
    df = df[df["date"] < cutoff]
    return {d: set(g["stock_id"]) for d, g in df.groupby("date")}


def delete_date(conn, table, d, store_ids):
    """單一日期、單一交易：鎖表 → 重新讀該日 keys → 只刪已驗證 keys。回傳 (deleted, kept)。"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(f"SELECT stock_id FROM {table} WHERE date = %s", (d,))
            db_ids = [r[0] for r in cur.fetchall()]
            ids = select_deletable(db_ids, store_ids)
            if ids:
                cur.execute(f"DELETE FROM {table} WHERE date = %s AND stock_id = ANY(%s)", (d, ids))
                if cur.rowcount != len(ids):
                    raise HistoryError(f"{table} {d} 刪除筆數 {cur.rowcount} != 預期 {len(ids)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(ids), len(db_ids) - len(ids)


def cmd_prune(args):
    if args.apply and args.store_dir:
        raise HistoryError("--apply 一律以 GitHub Release 為準，不可搭配 --store-dir")
    cutoff = months_cutoff(args.keep_months)
    log(f"[prune] cutoff = {cutoff}（刪除 date < cutoff）")
    tmp = None
    try:
        if args.store_dir:
            store_dir = args.store_dir
            log(f"[prune] dry-run，使用本機 store {store_dir}（不是 Release，僅供預覽）")
        else:
            tmp = tempfile.mkdtemp(prefix="hist_prune_")
            manifest, _ = download_release(tmp)  # 逐檔驗 sha256 / 列數
            if manifest is None or not manifest["files"]:
                raise HistoryError("Release 不存在或沒有資料檔，拒絕 prune")
            store_dir = tmp
            log(f"[prune] 已下載並驗證 Release 的 {len(manifest['files'])} 個檔案")
        conn = connect(readonly=True)
        try:
            if not verify(cutoff, conn, store_dir):
                log("[prune] verify 失敗，中止（不刪除任何資料）")
                return 1
            plan = {}
            with conn.cursor() as cur:
                for spec in KINDS.values():
                    t = spec["table"]
                    cur.execute(f"SELECT count(*) FROM {t}")
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT date, count(*) FROM {t} WHERE date < %s GROUP BY date ORDER BY date", (cutoff,))
                    per_date = cur.fetchall()
                    cur.execute("SELECT pg_total_relation_size(%s)", (t,))
                    size_mb = cur.fetchone()[0] / 1048576
                    n = sum(c for _, c in per_date)
                    plan[t] = [d for d, _ in per_date]
                    log(f"[prune] {t}: 刪除 {n:,}/{total:,} 筆, {len(per_date)} 個日期, "
                        f"估計可重用 ~{size_mb * n / max(total, 1):.0f} MB（表總大小 {size_mb:.0f} MB）")
                before = db_size_mb(cur)
            conn.rollback()
        finally:
            conn.close()
        if not args.apply:
            log(f"[prune] dry-run（未加 --apply），DB 目前 {before:.0f} MB")
            return 0
        keys = {spec["table"]: store_keys_by_date(k, store_dir, cutoff) for k, spec in KINDS.items()}
        conn = connect(readonly=False, session=True)
        try:
            for t, dates in plan.items():
                gone = kept = 0
                for i, d in enumerate(dates, 1):
                    g, k = delete_date(conn, t, d, keys[t].get(d, set()))
                    gone, kept = gone + g, kept + k
                    if i % 50 == 0:
                        log(f"  [prune] {t}: {i}/{len(dates)}")
                log(f"[prune] {t}: 已刪除 {gone:,} 筆，保留（store 無此 key）{kept:,} 筆")
            conn.commit()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
                for t in plan:
                    cur.execute(f"VACUUM ANALYZE {t}")
                mid = db_size_mb(cur)
                if args.vacuum_full:
                    # ACCESS EXCLUSIVE 鎖；重寫表需要約 2 倍表大小的剩餘磁碟空間
                    for t in plan:
                        try:
                            cur.execute(f"VACUUM FULL {t}")
                        except Exception as exc:
                            log(f"[prune] VACUUM FULL {t} 失敗（空間可能不足）：{str(exc)[:120]}")
                after = db_size_mb(cur)
            log(f"[prune] DB size {before:.0f} MB -> 一般 VACUUM 後 {mid:.0f} MB -> 最終 {after:.0f} MB")
            if not args.vacuum_full:
                log("[prune] 注意：一般 VACUUM 只讓空間可在表內重用，不會縮小檔案/資料庫大小；要真的縮小請加 --vacuum-full")
        finally:
            conn.close()
        return 0
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def cmd_purge_warrants(args):
    """從本機 store 移除權證列（之後 push 會縮小檔案，需 --allow-shrink 才能上傳）。"""
    for kind in KINDS:
        for path in year_files(kind):
            year = os.path.basename(path).split("_")[1].split(".")[0]
            df = read_year(kind, year)
            clean = drop_warrants(df)
            if len(clean) != len(df):
                write_year(kind, year, clean.reset_index(drop=True))
                log(f"[purge] {os.path.basename(path)}: {len(df):,} -> {len(clean):,}")
    return 0


# ── stats / query ─────────────────────────────────────────────────────────────
def cmd_stats(args):
    total = 0
    for kind in KINDS:
        for p in year_files(kind):
            s = file_summary(p)
            total += s["size"]
            log(f"{os.path.basename(p):22s} rows={s['rows']:>10,} {s['min_date']}~{s['max_date']} "
                f"ids={s['ids']:>6,} size={s['size'] / 1048576:7.1f} MB")
    log(f"total size = {total / 1048576:.1f} MB")
    return 0


def cmd_query(args):
    import duckdb
    con = duckdb.connect()
    base = HISTORY_DIR.replace("\\", "/")
    for kind in KINDS:
        if year_files(kind):
            con.execute(f"CREATE VIEW {kind} AS SELECT * FROM read_parquet('{base}/{kind}_*.parquet')")
    print(con.execute(args.sql).fetchdf().to_string())
    return 0


def main():
    ap = argparse.ArgumentParser(description="Parquet history store")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pull")
    p.add_argument("--repair", action="store_true", help="push 中途失敗後：依實際 assets 重建 manifest")
    p.add_argument("--allow-shrink", action="store_true")
    p.set_defaults(fn=cmd_pull)
    p = sub.add_parser("push")
    p.add_argument("--allow-shrink", action="store_true", help="允許新檔列數少於遠端（刻意縮減才用）")
    p.set_defaults(fn=cmd_push)
    p = sub.add_parser("export-db")
    p.add_argument("--since")
    p.set_defaults(fn=cmd_export_db)
    p = sub.add_parser("append")
    p.add_argument("--days", type=int, default=10)
    p.set_defaults(fn=cmd_append)
    p = sub.add_parser("backfill")
    p.add_argument("--years", type=int, default=5)
    p.add_argument("--start", help="YYYYMMDD（覆寫 --years）")
    p.add_argument("--end", help="YYYYMMDD")
    p.add_argument("--from-year", type=int, help="起始年（配 --to-year 可一年一次 dispatch）")
    p.add_argument("--to-year", type=int)
    p.add_argument("--kind", choices=["prices", "chips", "both"], default="both")
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sleep", type=float, default=1.5, help="每次請求前的等待秒數")
    p.set_defaults(fn=cmd_backfill)
    p = sub.add_parser("fix-tpex-chips")
    p.add_argument("--from-year", type=int)
    p.add_argument("--to-year", type=int)
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sleep", type=float, default=1.5)
    p.set_defaults(fn=cmd_fix_tpex_chips)
    p = sub.add_parser("verify")
    p.add_argument("--before", required=True)
    p.add_argument("--store-dir", help="預設本機 history/")
    p.set_defaults(fn=cmd_verify)
    p = sub.add_parser("prune")
    p.add_argument("--keep-months", type=int, default=13)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--vacuum-full", action="store_true")
    p.add_argument("--store-dir", help="僅 dry-run：以本機目錄取代下載 Release")
    p.set_defaults(fn=cmd_prune)
    sub.add_parser("stats").set_defaults(fn=cmd_stats)
    sub.add_parser("purge-warrants").set_defaults(fn=cmd_purge_warrants)
    p = sub.add_parser("query")
    p.add_argument("sql")
    p.set_defaults(fn=cmd_query)
    args = ap.parse_args()
    try:
        return args.fn(args) or 0
    except HistoryError as exc:
        log(f"[history] 中止：{exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
