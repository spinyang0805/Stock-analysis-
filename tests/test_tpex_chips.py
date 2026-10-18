import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
os.environ.pop("DATABASE_URL", None)

import jobs  # noqa: E402

INSTI_ROW = ['6488', '環球晶', '6,097,707', '8,211,537', '-2,113,830', '0', '0', '0',
             '6,097,707', '8,211,537', '-2,113,830', '306,600', '28,000', '278,600',
             '206,548', '388,030', '-181,482', '1,491,683', '1,589,873', '-98,190',
             '1,698,231', '1,977,903', '-279,672', '-2,114,902']
MARGIN_FIELDS = ['代號', '名稱', '前資餘額(張)', '資買', '資賣', '現償', '資餘額', '資屬證金',
                 '資使用率(%)', '資限額', '前券餘額(張)', '券賣', '券買', '券償', '券餘額',
                 '券屬證金', '券使用率(%)', '券限額', '資券相抵(張)', '備註']
MARGIN_ROW = ['6488', '環球晶', '15,796', '1,040', '2,841', '18', '13,977', '212', '10.58',
              '132,028', '422', '14', '151', '0', '285', '0', '0.21', '132,028', '11', '']


def _payload(date_roc, fields, row):
    return {"tables": [{"date": date_roc, "fields": fields, "data": [row]}]}


def _capture(monkeypatch, payload):
    sent = {}
    captured = []

    def fake_fetch(url, params=None, headers=None, retries=2):
        sent.update(params or {})
        return payload, None
    monkeypatch.setattr(jobs, "fetch_json", fake_fetch)
    monkeypatch.setattr(jobs, "save_chip_daily_bulk", lambda rows: (captured.extend(rows), len(rows))[1])
    return sent, captured


def test_insti_parse_columns():
    sid, doc = jobs._parse_tpex_insti_row(INSTI_ROW)
    assert sid == "6488"
    assert doc["foreign_buy"] == -2113830
    assert doc["investment_trust_buy"] == 278600
    assert doc["dealer_buy"] == -279672
    assert doc["institution_total_buy"] == -2114902


def test_insti_sends_roc_date(monkeypatch):
    sent, captured = _capture(monkeypatch, _payload("111/12/30", ["代號"] * 24, INSTI_ROW))
    res = {"errors": []}
    assert jobs.write_tpex_insti_chips("20221230", res) == 1
    assert sent["date"] == "111/12/30"
    assert captured[0][1] == "20221230"


def test_insti_rejects_other_day(monkeypatch):
    _, captured = _capture(monkeypatch, _payload("115/10/16", ["代號"] * 24, INSTI_ROW))
    res = {"errors": []}
    assert jobs.write_tpex_insti_chips("20221230", res) == 0
    assert not captured


def test_margin_reads_today_balance(monkeypatch):
    sent, captured = _capture(monkeypatch, _payload("115/10/15", MARGIN_FIELDS, MARGIN_ROW))
    res = {"errors": []}
    assert jobs.write_tpex_margin_chips("20261015", res) == 1
    assert sent["date"] == "115/10/15"
    doc = captured[0][2]
    assert doc["margin_balance"] == 13977  # 資餘額, not 前資餘額 15,796
    assert doc["short_balance"] == 285     # 券餘額, not 前券餘額 422


def test_yf_bar_skips_closed_and_zero_volume(monkeypatch):
    monkeypatch.setattr(jobs.trading_calendar, "is_trading_day", lambda d: d != "20260710")
    assert jobs._yf_bar_ok("20260709", {"Volume": 1200.0})
    assert not jobs._yf_bar_ok("20260710", {"Volume": 1200.0})  # 颱風休市
    assert not jobs._yf_bar_ok("20260709", {"Volume": 0.0})
    assert not jobs._yf_bar_ok("20260709", {"Volume": None})
