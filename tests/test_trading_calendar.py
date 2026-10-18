"""trading_calendar 與 jobs 誤標日期防護的測試（不連網路、不連 DB）。"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
import pytest  # noqa: E402

import trading_calendar as cal  # noqa: E402

# 取自 TWSE holidaySchedule 2026 實際回應（含標記列：開始/最後交易日 = 交易日）
API_2026 = {"data": [
    ["2026-01-01", "中華民國開國紀念日", ""],
    ["2026-01-02", "國曆新年開始交易日", ""],
    ["2026-02-11", "農曆春節前最後交易日", ""],
    ["2026-02-12", "市場無交易，僅辦理結算交割作業", ""],
    ["2026-02-13", "市場無交易，僅辦理結算交割作業", ""],
    *[[f"2026-02-{d}", "農曆除夕及春節", ""] for d in (16, 17, 18, 19, 20)],
    ["2026-02-23", "農曆春節後開始交易日", ""],
    ["2026-02-27", "和平紀念日", ""],
    ["2026-04-03", "兒童節及民族掃墓節", ""],
    ["2026-04-06", "兒童節及民族掃墓節", ""],
    ["2026-05-01", "勞動節", ""],
    ["2026-06-19", "端午節", ""],
    ["2026-09-25", "中秋節", ""],
    ["2026-09-28", "孔子誕辰紀念日/ 教師節", ""],
]}


@pytest.fixture
def api_calendar(monkeypatch):
    hol, years = cal._parse_api(API_2026)
    monkeypatch.setitem(cal._state, "loaded", True)
    monkeypatch.setitem(cal._state, "holidays", hol)
    monkeypatch.setitem(cal._state, "years", years)
    monkeypatch.setitem(cal._state, "source", "api")


@pytest.mark.parametrize("d", ["20260101", "20260212", "20260213", "20260216", "20260220",
                               "20260227", "20260403", "20260406", "20260501", "20260619",
                               "20260925", "20260928"])
def test_2026_holidays_are_not_trading_days(api_calendar, d):
    assert not cal.is_trading_day(d)


@pytest.mark.parametrize("d", ["20260102", "20260211", "20260223", "20260105", "20260616"])
def test_markers_and_normal_weekdays_trade(api_calendar, d):
    assert cal.is_trading_day(d)


def test_weekend_not_trading_but_makeup_saturday_is(api_calendar):
    assert not cal.is_trading_day("20260523")  # Saturday
    assert not cal.is_trading_day("20260517")  # Sunday
    assert cal.is_trading_day("20170930")      # 補行交易的週六


def test_trading_days_range(api_calendar):
    assert cal.trading_days("20260209", "20260224") == [
        "20260209", "20260210", "20260211", "20260223", "20260224"]


def test_coverage_and_source(api_calendar):
    assert cal.calendar_source() == "api"
    assert cal.covers("20260928")
    assert not cal.covers("20250101")  # API 只服務當年度；未涵蓋年份僅能確定週末


def test_fallback_is_weekday_only(monkeypatch):
    monkeypatch.setitem(cal._state, "loaded", True)
    monkeypatch.setitem(cal._state, "holidays", {})
    monkeypatch.setitem(cal._state, "years", set())
    monkeypatch.setitem(cal._state, "source", "fallback")
    assert cal.calendar_source() == "fallback"
    assert cal.is_trading_day("20260928")      # 無資料 → 平日視為交易日
    assert not cal.is_trading_day("20260927")  # 週日


def test_jobs_payload_date_mismatch():
    import jobs
    assert jobs._payload_date_mismatch({"date": "20261016"}, "20260928") == "20261016"
    assert jobs._payload_date_mismatch({"tables": [{"date": "115/10/16"}]}, "20261016") is None
    assert jobs._payload_date_mismatch({}, "20260928") is None


def test_write_twse_day_skips_mismatched_date(monkeypatch):
    import jobs
    payload = {"fields": ["日期", "證券代號"], "data": [["1151016", "2330"]]}
    monkeypatch.setattr(jobs, "fetch_json", lambda *a, **k: (payload, None))
    monkeypatch.setattr(jobs, "save_stock_daily_bulk", lambda rows: pytest.fail("must not write"))
    res = {"errors": []}
    assert jobs._write_twse_day("20260928", res) == (0, False)
    assert "skipped" in res["errors"][0]


def test_tpex_chip_writers_skip_mismatched_date(monkeypatch):
    import jobs
    payload = {"date": "20261016", "tables": [{"date": "115/10/16", "fields": ["代號", "名稱"],
                                               "data": [["1240"] + ["x"] * 30]}]}
    monkeypatch.setattr(jobs, "fetch_json", lambda *a, **k: (payload, None))
    monkeypatch.setattr(jobs, "save_chip_daily_bulk", lambda rows: pytest.fail("must not write"))
    for fn in (jobs.write_tpex_insti_chips, jobs.write_tpex_margin_chips):
        res = {"errors": []}
        assert fn("20260928", res) == 0
        assert "skipped" in res["errors"][0]
