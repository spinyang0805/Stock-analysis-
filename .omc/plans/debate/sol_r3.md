# Codex Sol — Round 3 (summary, faithful)
1. Cannot close yet; only an interim allocation. Evidence supports "combo not proven for real money".
2. Account period is 2020-01-02..2026-10-16 (~6.8 y), not 8 y. Data backfill and trade-level facts (max hold 484 d,
   +821%, worst −27%) not independently verified — need reproducible trade lists.
3. BUG: with settle=2 the daily equity omits unsettled sale proceeds → fake drawdowns; A_T2 MDD −59%/−61% invalid.
   Test: zero costs, flat prices, sell then wait — equity must stay flat.  [moderator: fixed]
4. T+2 does not freeze same-day proceeds: TWSE nets receivables/payables per settlement date; same-day sell+buy share
   the settlement date. settle=2 is a conservative stress case (and was not applied to 0050 parking proceeds).
5. MC shuffles the whole day's candidate list (not only RS ties) → measures order sensitivity, not a probability of
   future performance; 40 seeds share one market path (each run = 2.5 pts). Report frozen RS order, random orders, and
   time-block uncertainty side by side.
6. Q1 stop-loss: no evidence for a best %; 10% does not prevent long holds (it is from entry, no time limit). Keep 10%
   as the frozen research setting; don't drop risk control because "no stop" has the highest median. Note stop=0 in
   code = exit at entry price, not "no stop". Test with paired seeds, difference intervals, tail losses.
7. Q2: a stock CAN become unsellable (gaps, limit-down runs, suspension); 10% is a trigger, not a loss cap; −27% worst
   seen is not a floor. Needs delisted names, suspensions, liquidation values, multi-day no-fill stress tests.
8. Q3 per-stock AI: per-stock best-rule picking is refuted; a SHARED (pooled) regularised model using stock traits
   (volatility, liquidity, size, sector, trend, market state) to adjust entry score or size is still worth ONE
   controlled test: frozen simple model, few features, point-in-time data, outer yearly walk-forward, inner tuning,
   purged overlapping labels, a held-out group of stocks; judge by account CAGR/MDD/tail risk, not accuracy.
9–11. Cross-exam: A single +33.7%/−41% vs MC median 23.7%/−38% → no real-money case for MA60 slots. 80/20 median
   27.2%/−31% (−1.3 pts CAGR, +3 pts MDD) fails the double gate; no-rebalance blend weights drift. Old Luna/Haiku
   numbers predate the data fix → re-attribute. "Rank 1 = biggest traded value" in diagnostics. 0050 timing 25.5%/−21%
   is a return/drawdown trade-off, not worthless. A blend can't beat its best part on CAGR but can improve drawdown.
12. Remaining doubts: metrics() normalises by the first day's close (initial costs hidden) [moderator: fixed];
   reproducibility (data dates, code version, params, trade lists, adjustments, delisted coverage); 500-day cap on/off
   per seed; the user's acceptable drawdown is undefined — the double gate is not the only valid utility.
13. Interim real-money advice: unlevered 0050 as baseline; combo paper-only.
14. Worth testing: (a) 0050 B&H, (b) 0050 above TAIEX 240-MA else cash, (c) 50% 0050 + 50% 00631L above 240-MA else
   cash (~1.5× exposure). Use real ETF prices, next-open fills, costs, consistent settlement, neighbouring MA lengths
   for stability, and forward validation. 00631L targets 2× DAILY returns (compounding/tracking differences).
