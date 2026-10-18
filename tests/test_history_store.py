"""history_store 的資料安全測試（不連 DB、不連 GitHub：gh 以假 release 取代）。
執行：python -m pytest tests/test_history_store.py
"""
import json
import os
import shutil
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
import history_store as hs  # noqa: E402


class FakeRelease:
    """模擬 gh release view/download/upload/create；assets 存在 self.dir。"""

    def __init__(self, tmp_path, exists=True):
        self.dir = tmp_path / "remote"
        self.dir.mkdir()
        self.exists = exists
        self.fail_view = None  # 設成字串即讓 view 回非 not-found 錯誤

    def __call__(self, *args, check=True):
        ok = lambda out="": SimpleNamespace(returncode=0, stdout=out, stderr="")  # noqa: E731
        bad = lambda err: SimpleNamespace(returncode=1, stdout="", stderr=err)  # noqa: E731
        if args[:2] == ("release", "view"):
            if self.fail_view:
                return bad(self.fail_view)
            if not self.exists:
                return bad("release not found")
            return ok(json.dumps({"assets": [{"name": n} for n in os.listdir(self.dir)]}))
        if args[:2] == ("release", "create"):
            self.exists = True
            return ok()
        if args[:2] == ("release", "download"):
            dest, pat = args[args.index("--dir") + 1], args[args.index("--pattern") + 1]
            src = self.dir / pat
            if not src.exists():
                return bad("no assets match")
            shutil.copy(src, os.path.join(dest, pat))
            return ok()
        if args[:2] == ("release", "upload"):
            shutil.copy(args[3], self.dir / os.path.basename(args[3]))
            return ok()
        raise AssertionError(args)


def prices(n, year="2025", offset=0):
    return pd.DataFrame({"stock_id": [str(1000 + i + offset) for i in range(n)], "date": f"{year}0102",
                         "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})


@pytest.fixture
def env(tmp_path, monkeypatch):
    rel = FakeRelease(tmp_path)
    monkeypatch.setattr(hs, "gh", rel)
    monkeypatch.setattr(hs, "HISTORY_DIR", str(tmp_path / "local"))
    return rel, tmp_path


def seed_remote(rel, n):
    """在假 release 放入 prices_2025（n 列）+ manifest。"""
    stage = rel.dir.parent / "stage"
    stage.mkdir(exist_ok=True)
    old = hs.HISTORY_DIR
    hs.HISTORY_DIR = str(stage)
    try:
        hs.merge_into_store("prices", prices(n))
        s = hs.file_summary(hs.year_path("prices", "2025"))
    finally:
        hs.HISTORY_DIR = old
    shutil.copy(stage / "prices_2025.parquet", rel.dir / "prices_2025.parquet")
    (rel.dir / "manifest.json").write_text(json.dumps({"files": {"prices_2025.parquet": s}, "nontrading_dates": []}))


def args(**kw):
    return SimpleNamespace(allow_shrink=kw.get("allow_shrink", False))


def test_pull_release_not_found_is_ok(env):
    rel, _ = env
    rel.exists = False
    assert hs.cmd_pull(None) == 0
    assert json.load(open(os.path.join(hs.HISTORY_DIR, hs.PULLED)))["release_missing"] is True


def test_pull_fails_hard_on_other_errors(env):
    rel, _ = env
    rel.fail_view = "HTTP 502: Bad Gateway"
    with pytest.raises(hs.HistoryError):
        hs.cmd_pull(None)


def test_pull_fails_on_sha_mismatch(env):
    rel, _ = env
    seed_remote(rel, 5)
    with open(rel.dir / "prices_2025.parquet", "ab") as f:
        f.write(b"corrupt")
    with pytest.raises(hs.HistoryError):
        hs.cmd_pull(None)


def test_push_refuses_without_pulled_state(env):
    rel, _ = env
    seed_remote(rel, 5)
    hs.merge_into_store("prices", prices(10))
    with pytest.raises(hs.HistoryError):
        hs.cmd_push(args())


def test_push_refuses_to_shrink_but_allows_growth(env):
    rel, _ = env
    seed_remote(rel, 50)
    hs.cmd_pull(None)
    # 本地被換成較小的檔（例如 pull 不完整或誤覆寫）
    os.remove(hs.year_path("prices", "2025"))
    hs.merge_into_store("prices", prices(10))
    before = (rel.dir / "prices_2025.parquet").read_bytes()
    with pytest.raises(hs.HistoryError):
        hs.cmd_push(args())
    assert (rel.dir / "prices_2025.parquet").read_bytes() == before  # 遠端未被動到
    # 成長則可以，且上傳後驗證通過
    os.remove(hs.year_path("prices", "2025"))
    hs.merge_into_store("prices", prices(80))
    assert hs.cmd_push(args()) == 0
    assert hs.read_year("prices", "2025").shape[0] == 80
    assert json.load(open(rel.dir / "manifest.json"))["files"]["prices_2025.parquet"]["rows"] == 80


def test_push_refuses_when_remote_changed_since_pull(env):
    rel, _ = env
    seed_remote(rel, 5)
    hs.cmd_pull(None)
    hs.merge_into_store("prices", prices(20))
    m = json.load(open(rel.dir / "manifest.json"))
    m["generated_at"] = "someone else pushed"
    (rel.dir / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(hs.HistoryError):
        hs.cmd_push(args())


def test_merge_policy_old_keeps_existing_rows(env):
    hs.merge_into_store("prices", prices(3))
    changed = prices(3)
    changed["close"] = 99.0
    hs.merge_into_store("prices", changed, prefer="old")
    assert (hs.read_year("prices", "2025")["close"] == 1.0).all()
    hs.merge_into_store("prices", changed, prefer="new")
    assert (hs.read_year("prices", "2025")["close"] == 99.0).all()


def test_select_deletable_only_verified_keys():
    # DB 在 verify 之後又多出 "9999"（store 沒有）→ 不能刪
    assert hs.select_deletable(["1101", "2330", "9999"], {"1101", "2330", "0050"}) == ["1101", "2330"]
    assert hs.select_deletable(["9999"], set()) == []


def test_delete_date_deletes_only_verified_keys():
    class Cur:
        def __init__(self):
            self.rowcount = 0
            self.deleted = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            if sql.startswith("DELETE"):
                self.deleted = params[1]
                self.rowcount = len(params[1])

        def fetchall(self):
            return [("1101",), ("2330",), ("NEW1",)]

    class Conn:
        def __init__(self):
            self.cur = Cur()
            self.committed = False

        def cursor(self):
            return self.cur

        def commit(self):
            self.committed = True

        def rollback(self):
            raise AssertionError("不應 rollback")

    c = Conn()
    assert hs.delete_date(c, "stock_daily", "20240102", {"1101", "2330"}) == (2, 1)
    assert c.cur.deleted == ["1101", "2330"] and c.committed


def test_merge_old_fills_null_columns_only(env):
    base = pd.DataFrame({"stock_id": ["2330", "2317"], "date": "20250102", "name": ["TSMC", "HonHai"],
                         "market": "TWSE", "foreign_buy": [100, None], "margin_balance": [None, None]})
    hs.merge_into_store("chips", base)
    fill = pd.DataFrame({"stock_id": ["2330", "2317", "2454"], "date": "20250102",
                         "foreign_buy": [999, 7, 5], "margin_balance": [50, 60, 70]})
    hs.merge_into_store("chips", fill, prefer="old")
    got = hs.read_year("chips", "2025").set_index("stock_id")
    assert got.loc["2330", "foreign_buy"] == 100          # 既有非空值不被覆蓋
    assert got.loc["2330", "margin_balance"] == 50        # 既有 NULL 被補上
    assert got.loc["2317", "foreign_buy"] == 7 and got.loc["2317", "margin_balance"] == 60
    assert got.loc["2330", "name"] == "TSMC"
    assert "2454" in got.index                            # 新 key 照常新增


def test_is_warrant_keeps_etf_etn_tdr_preferred():
    for code in ["711234", "030001", "08031P", "700001", "72xxxx"[:6].replace("x", "0")]:
        assert hs.is_warrant(code), code
    for code in ["2330", "1101B", "00631L", "00878", "00992A", "006201", "020000", "910322", "01001T", "2887Z1"]:
        assert not hs.is_warrant(code), code
    df = pd.DataFrame({"stock_id": ["2330", "711234", "00878"]})
    assert list(hs.drop_warrants(df)["stock_id"]) == ["2330", "00878"]


def swap_remote_asset(rel, n):
    """模擬 push 中途失敗：資料檔已換成 n 列，但 manifest 還是舊的。"""
    stage = rel.dir.parent / "stage2"
    stage.mkdir(exist_ok=True)
    old = hs.HISTORY_DIR
    hs.HISTORY_DIR = str(stage)
    try:
        hs.merge_into_store("prices", prices(n))
    finally:
        hs.HISTORY_DIR = old
    shutil.copy(stage / "prices_2025.parquet", rel.dir / "prices_2025.parquet")


def test_repair_rebuilds_manifest_after_partial_push(env):
    rel, _ = env
    seed_remote(rel, 50)
    swap_remote_asset(rel, 80)
    with pytest.raises(hs.HistoryError):  # 一般 pull 因 sha 不符而失敗
        hs.cmd_pull(SimpleNamespace(repair=False))
    hs.cmd_pull(SimpleNamespace(repair=True, allow_shrink=False))
    assert hs.cmd_push(args()) == 0  # 上傳修復後的 manifest
    assert json.load(open(rel.dir / "manifest.json"))["files"]["prices_2025.parquet"]["rows"] == 80
    assert hs.cmd_pull(SimpleNamespace(repair=False)) == 0  # 之後 pull 恢復正常


def test_repair_refuses_shrink_and_missing_asset(env):
    rel, _ = env
    seed_remote(rel, 50)
    swap_remote_asset(rel, 10)  # 實際比 manifest 少 → 疑似資料遺失
    with pytest.raises(hs.HistoryError):
        hs.cmd_pull(SimpleNamespace(repair=True, allow_shrink=False))
    os.remove(rel.dir / "prices_2025.parquet")  # 缺檔
    with pytest.raises(hs.HistoryError):
        hs.cmd_pull(SimpleNamespace(repair=True, allow_shrink=True))


def test_gh_retry_retries_then_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(hs.time, "sleep", lambda s: None)
    monkeypatch.setattr(hs, "gh", lambda *a, check=True: calls.append(a) or SimpleNamespace(returncode=1, stdout="", stderr="x"))
    with pytest.raises(hs.HistoryError):
        hs.gh_retry("release", "upload", "t", "f")
    assert len(calls) == 3
