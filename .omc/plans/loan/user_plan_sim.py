"""User's own plan: borrow 2,000,000 @2.9%, 120 months (pay 19,220/mo).
20萬 bank for payments, 30萬 reserve, 150萬 into 0050 (total return, from codex_repro_vt load_prices).
Payments drawn: bank → reserve → sell 0050 (worst case: no salary). Cash earns 1.5%/yr (0% sensitivity).
Rolling 10-year windows, start each month 2004-01 … 2016-10. Reports terminal 0050 value after loan fully repaid."""
import sys, importlib.util
import numpy as np, pandas as pd
spec = importlib.util.spec_from_file_location("r", ".omc/plans/debate/codex_repro_vt.py"); r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
p = r.load_prices()["close"]
m = p.resample("ME").last()
P, rate, n = 2_000_000, 0.029/12, 120
pay = P*rate/(1-(1+rate)**-n)
def run(i0, cash_rate, stock0=1_500_000, cash0=500_000):
    px = m.iloc[i0:i0+n+1].to_numpy()
    units = stock0*(1-0.0015)/px[0]; cash = cash0; low = 1e18; sold = 0
    for k in range(1, n+1):
        cash *= 1+cash_rate/12
        need = pay
        use = min(cash, need); cash -= use; need -= use
        if need > 0:
            u = need/(px[k]*(1-0.0015-0.001)); units -= u; sold += need
        low = min(low, units*px[k]+cash)
    return units*px[n]+cash, low, sold
print(f"monthly payment {pay:,.0f}; total paid {pay*n:,.0f}")
for cr in (0.015, 0.0):
    rows = []
    for i0 in range(len(m)-n):
        if m.index[i0] < pd.Timestamp("2004-01-01"): continue
        end, low, sold = run(i0, cr)
        rows.append((m.index[i0].strftime("%Y-%m"), end, low, sold))
    d = pd.DataFrame(rows, columns=["start","end","low","sold"])
    print(f"\ncash rate {cr:.1%}: windows {len(d)} ({d.start.iloc[0]}–{d.start.iloc[-1]})")
    print(" terminal 0050 value after loan repaid (no salary used): min {:,.0f} p10 {:,.0f} median {:,.0f} max {:,.0f}".format(d.end.min(), d.end.quantile(.1), d.end.median(), d.end.max()))
    print(" share of windows ending below 0:", (d.end<=0).mean(), "  below 500k:", round((d.end<500_000).mean(),3))
    print(" worst 3 starts:", d.nsmallest(3,"end")[["start","end","low"]].to_string(index=False))
    print(" lowest mid-way total value:", f"{d.low.min():,.0f}", "at start", d.loc[d.low.idxmin(),"start"])

# salary share sensitivity: salary pays s × payment; the rest from bank → reserve → 0050
def run2(i0, s, cash_rate=0.015):
    px = m.iloc[i0:i0+n+1].to_numpy(); units = 1_500_000*(1-0.0015)/px[0]; cash = 500_000; mins = 1e18
    for k in range(1, n+1):
        cash *= 1+cash_rate/12; need = pay*(1-s)
        use = min(cash, need); cash -= use; need -= use
        if need > 0: units -= need/(px[k]*(1-0.0025))
        mins = min(mins, units*px[k])
    return units*px[n]+cash, mins
for s in (0.5, 1.0):
    res = [run2(i0, s) for i0 in range(len(m)-n) if m.index[i0] >= pd.Timestamp("2004-01-01")]
    e = np.array([x[0] for x in res]); salary = pay*n*s
    print(f"\nsalary pays {s:.0%} ({pay*s:,.0f}/mo, {salary:,.0f} total): terminal min {e.min():,.0f} p10 {np.quantile(e,.1):,.0f} "
          f"median {np.median(e):,.0f}; net of salary paid: min {e.min()-salary:,.0f} median {np.median(e)-salary:,.0f}; "
          f"share net<0 {(e-salary<0).mean():.2f}; worst 0050 value mid-way {min(x[1] for x in res):,.0f}")
