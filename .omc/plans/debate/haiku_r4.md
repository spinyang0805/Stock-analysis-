# Claude Haiku (max) — Round 4 (summary, faithful)
Independently rebuilt 0050 adjusted series and timing from repo caches: 2004+ B&H and MA200/240/280 match the report;
2015+ MA280 16.8%/−24.9% vs report 16.1%/−25% (data version, D7).
Q1 period: risk decisions on 2004+ (2008 −55.8%, 2004–06 chop, all-real 0050). 2015+ is a single low-vol regime
(2015–19 vol 14.5% vs 2004–08 25.8%). TSMC 15%→57% unverifiable in repo; 0050–TAIEX corr 0.951. Neither dismissible.
Q2: MA200 2004+ zero-cost 10.7%/−29.6%, with costs 8.6%/−37.4% (92 round trips, ~2.1%/yr drag) ≈ fixed 60% monthly
8.6%/−37.5%. Timing avoided 2008 (+30 pp) and 2022 (+12 pp) but lagged 2009/2010/2012 chop → net ≈ 0 on 2004+.
Start-date sensitive: MA200 from 2005 → 9.7%/−27.6%; from 2009 13.1%/−21.3%; B&H ≈ −56% from every start.
"2020–26 beats on both": 0050 MA240 only wins on MDD. 50/50 relies on real 00631L which lagged 2×0050 in 2024–26.
Two frozen rules to test once on 2004+:
 R1 vol target: E = min(1, 0.20 / (21-day stdev of 0050 daily returns × √252)), weekly last close, next open, cash 0%.
 R2 MA200 no-trade band: enter when TAIEX close > 1.02×MA200, exit when < 0.98×MA200, else keep; 0050 or cash.
Q3 fixed weights (2004+, costs, month-end; NOTE: Haiku computed a grid 50–65%): 50% 7.2%/−32.2%; 55% 7.9%/−34.9%;
60% 8.6%/−37.5%; 65% 9.2%/−40.1%; 100% 13.8%/−55.8% → max ≈55% under 35%. R1 must beat F55 (MDD ≤35% and >7.9%).
No 0050/2× mixes (synthetic 2004–14, unstable tracking; real 00631L −2.6%/yr vs 2×0050, big lag 2024–26).
Q4 combo: paper only; stock data only from 2019 → no 2004+ test; RS order may be in-sample.
Doubts: D1 00631L tracking vs official NAV; D2 report 2004/2005/2006 starts; D3 timing costs (need >1%/yr net);
D4 cash interest ~1% adds 0.4–0.5 pp to F55; D5 CPPI 'dead' flag never re-enters (2008 likely triggers) → not
comparable; D6 timing value rests on 2008 & 2022; D7 market_index updated today — freeze data hash; D8 TSMC weights.
Withdraws own round-3 "B (0050 MA240) is the candidate" — holds only on 2015+/2020+.
Real money now: 0050 55% (cap) or 50% (buffer), rest cash, month-end rebalance; no 00631L, no combo.
 55%: 2004+ 7.9%/−34.9%; 2015+ 11.5%/−20.0%; 2020+ 15.5%/−20.0%. 50%: 7.2%/−32.2%. Not final (D1–D3, D5, D7 open).
