"""probe_tpex.py — Diagnose TPEx endpoints from whatever host runs this.
Prints status/content-type/first bytes for each candidate URL+params so we can
see what the API actually returns now (JSON? CSV? HTML? param change?).

Used by .github/workflows/probe.yml as the "測一測再寫解析" step for the
2026-07-14 pipeline-fix spec: T1 (批次日K) + T2 (個股月K tradingStock 參數)。
"""
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html",
    "Origin": "https://www.tpex.org.tw",
}

CASES = [
    # ── control: known-good endpoint (should stay JSON) ─────────────────────
    ("insti dailyTrade (known-good)", "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
     {"response": "json", "date": "20260713", "sect": "AL", "type": "Daily"}),

    # ── T1: batch daily close quotes — OpenAPI candidates (首選) ─────────────
    ("openapi swagger.json", "https://www.tpex.org.tw/openapi/swagger.json", None),
    ("openapi mainboard_daily_close_quotes", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes", None),
    ("openapi esb_daily_close_quotes (興櫃?)", "https://www.tpex.org.tw/openapi/v1/tpex_esb_daily_close_quotes", None),
    ("openapi mainboard_quotes", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", None),
    ("openapi etf_daily_close_quotes", "https://www.tpex.org.tw/openapi/v1/tpex_etf_daily_close_quotes", None),

    # ── T1: www afterTrading/ path variants (fallback if OpenAPI fields lack) ─
    ("dailyCloseQuotes roc-slash (old, expect 404)", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "json", "date": "115/07/13"}),
    ("dailyMarketQuotes roc-slash", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyMarketQuotes",
     {"response": "json", "date": "115/07/13"}),
    ("dailyQuotes roc-slash", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",
     {"response": "json", "date": "115/07/13"}),
    ("stockDayAll roc-slash", "https://www.tpex.org.tw/www/zh-tw/afterTrading/stockDayAll",
     {"response": "json", "date": "115/07/13"}),

    # ── T2: individual stock monthly K (tradingStock) param combos ──────────
    ("tradingStock code+iso-date", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "date": "2026/07/01"}),
    ("tradingStock code+roc-date-01", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "date": "115/07/01"}),
    ("tradingStock code+roc-ym", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "date": "115/07"}),
    ("tradingStock code+id+l", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "id": "", "l": "zh-tw", "date": "115/07/01"}),
    ("tradingStock stockNo+roc-date-01", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "stockNo": "6488", "date": "115/07/01"}),
    ("tradingStock no-l-param", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "date": "115/07/01", "l": "zh-tw"}),

    # ── T2: OpenAPI individual-stock history candidates ──────────────────────
    ("openapi mainboard_quotes_after_hour", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes_after_hour", None),
    ("openapi stock_dailyquotes", "https://www.tpex.org.tw/openapi/v1/tpex_stock_dailyquotes", None),
]

for label, url, params in CASES:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        body = r.text[:300].replace("\n", "\\n")
        print(f"== {label}\n   GET {r.url}\n   {r.status_code} {r.headers.get('content-type')}\n   {body}\n")
    except Exception as exc:
        print(f"== {label}\n   ERROR {exc}\n")
