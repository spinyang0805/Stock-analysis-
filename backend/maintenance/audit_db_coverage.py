"""audit_db_coverage.py — READ-ONLY coverage audit of Supabase vs the Parquet history store and the calendar.

Writes a CSV/markdown bundle (default history/out/audit/) with, per trading date and market:
  stock_daily rows, rows with valid OHLC/volume, chip_daily rows and per-field non-null counts
  (foreign / trust / dealer / margin / short / institution_total), store rows, expected trading day flag
plus per-table extremes (dates with 0 rows, dates far below the median) and fundamentals freshness.

  python backend/maintenance/audit_db_coverage.py [--out DIR]
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

import trading_calendar as cal  # noqa: E402
from delete_nontrading import connect  # noqa: E402

HISTORY = os.path.join(os.path.dirname(BACKEND), "history")


def q(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HISTORY, "out", "audit"))
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
    sd = q(conn, """SELECT date, coalesce(market,'?') market, count(*) n,
                           count(*) FILTER (WHERE open>0 AND high>0 AND low>0 AND close>0) ohlc_ok,
                           count(*) FILTER (WHERE volume>0) vol_pos,
                           count(*) FILTER (WHERE stock_id ~ '^[0-9]{4}$') four_digit,
                           count(*) FILTER (WHERE source ILIKE 'yfinance%%') yf
                    FROM stock_daily GROUP BY 1,2 ORDER BY 1,2""")
    cd = q(conn, """SELECT date, coalesce(market,'?') market, count(*) n,
                           count(foreign_buy) foreign_nn, count(investment_trust_buy) trust_nn,
                           count(dealer_buy) dealer_nn, count(margin_balance) margin_nn,
                           count(short_balance) short_nn, count(institution_total_buy) inst_total_nn
                    FROM chip_daily GROUP BY 1,2 ORDER BY 1,2""")
    fu = q(conn, """SELECT valuation_date, revenue_date, count(*) n FROM fundamentals GROUP BY 1,2 ORDER BY 3 DESC""")
    span = q(conn, "SELECT min(date) mn, max(date) mx FROM stock_daily")
    conn.rollback()
    conn.close()
    lo, hi = span["mn"].iloc[0], span["mx"].iloc[0]
    glob = os.path.join(HISTORY, "prices_*.parquet").replace("\\", "/")
    st = duckdb.sql(f"""SELECT date, market, count(*) store_n FROM read_parquet('{glob}', union_by_name=true)
                        WHERE date BETWEEN '{lo}' AND '{hi}' GROUP BY 1,2""").df()
    days = pd.DataFrame({"date": cal.trading_days(lo, hi)})
    days["calendar_source"] = cal.calendar_source()
    sd.to_csv(os.path.join(a.out, "stock_daily_by_date.csv"), index=False)
    cd.to_csv(os.path.join(a.out, "chip_daily_by_date.csv"), index=False)
    st.to_csv(os.path.join(a.out, "store_prices_by_date.csv"), index=False)
    days.to_csv(os.path.join(a.out, "trading_days.csv"), index=False)
    fu.to_csv(os.path.join(a.out, "fundamentals_freshness.csv"), index=False)
    # quick summary
    L = [f"# DB coverage audit {lo}..{hi}", ""]
    for name, df in (("stock_daily", sd), ("chip_daily", cd)):
        piv = df.pivot_table(index="date", columns="market", values="n", aggfunc="sum").fillna(0)
        miss = sorted(set(days["date"]) - set(piv.index))
        L.append(f"## {name}: {len(piv)} dates; trading days with NO rows: {len(miss)} {miss[:20]}")
        for mk in piv.columns:
            s = piv[mk]
            med = s.median()
            low = s[s < 0.7 * med]
            L.append(f"- {mk}: median {med:.0f}/day; dates < 70% of median: {len(low)} "
                     f"{dict(list(low.astype(int).items())[:15])}")
        L.append("")
    m = sd.groupby(["date", "market"])["n"].sum().reset_index().merge(st, on=["date", "market"], how="outer")
    m["gap"] = m["store_n"].fillna(0) - m["n"].fillna(0)
    big = m[m["gap"] > 20].sort_values("gap", ascending=False)
    L.append(f"## store rows minus DB rows > 20: {len(big)} (date, market) pairs; top: "
             f"{big.head(15)[['date', 'market', 'n', 'store_n']].to_dict('records')}")
    with open(os.path.join(a.out, "summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))
    print(f"\nwritten to {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
