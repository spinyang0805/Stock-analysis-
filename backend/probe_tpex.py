"""probe_tpex.py — Diagnose TPEx endpoints from whatever host runs this.
Prints status/content-type/first bytes for each candidate URL+params so we can
see what the API actually returns now (JSON? CSV? HTML? param change?).
"""
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html",
    "Origin": "https://www.tpex.org.tw",
}

CASES = [
    ("dailyCloseQuotes roc-slash", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "json", "date": "115/07/13"}),
    ("dailyCloseQuotes yyyymmdd", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "json", "date": "20260713"}),
    ("dailyCloseQuotes iso", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "json", "date": "2026/07/13"}),
    ("dailyCloseQuotes no-date", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "json"}),
    ("dailyCloseQuotes csv", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes",
     {"response": "csv", "date": "115/07/13"}),
    ("tradingStock 6488 Jul", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
     {"response": "json", "code": "6488", "date": "115/07/01"}),
    ("insti dailyTrade (known-good)", "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
     {"response": "json", "date": "20260713", "sect": "AL", "type": "Daily"}),
]

for label, url, params in CASES:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        body = r.text[:280].replace("\n", "\\n")
        print(f"== {label}\n   {r.status_code} {r.headers.get('content-type')}\n   {body}\n")
    except Exception as exc:
        print(f"== {label}\n   ERROR {exc}\n")
