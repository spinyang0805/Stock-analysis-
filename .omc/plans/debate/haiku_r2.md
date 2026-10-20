# Claude Haiku (max effort) — Round 2 (summary, faithful)
Confirms: Sol's MA60 regime slot caps (best single change); per-trade≠portfolio; brief/code mismatch; event/state
mixing; Luna's missing-market-data=0 risk and alias double counting. Validation half-year is NOT out-of-sample: SPLIT
and strict B were committed together (35f883dc1, 12 min after the layered rules) — no proof rules were frozen first.
Rejects: rank priority (14.3%), profit trail (+2.1%/trade; refutes "strict B gives back gains"), ATR+0.5% risk
(lower CAGR from lower exposure 60%), own MA240 half-slots (17.7%), own "N chosen 2019–23" (2024–26 all N lag badly).
Corrects own view: slippage ≈1.2 pts/yr only; zero-slip still −7 pts vs 0050 → "about equal" was wrong.
Why it lags: 2020–23 combo ≈3.1× vs 0050 1.6×; 2024→2026-10 combo ≈1.1× vs 0050 3.5×. Idle cash explains ~22 pts of
2024's 34-pt gap. Hypothesis: top-500 turnover + RS ordering tilts away from TSMC/AI large caps. 27% win → depends on a
few big winners; N5 vs N10 differ 76 pts in 2024 → very noisy.
Simulator biases (optimistic): (a) T+0 reuse of sale cash (TW settles T+2); (b) fast exit counts warning-only signals
(s_ma20_break, s_macd_down) and counts signals not categories → D's 364 trades / 2022 −32%; (c) only one-price bars
treated as locked; (d) delisted names missing (run_tuning.py notes it); (e) skipped gap-ups / full slots not
back-filled — measure their later returns; (f) s_pat_bear_struct may double count with lh/ll.
Candidates (pass = CAGR ≥ 0050 AND MDD not deeper; freeze code first):
 A: combo N10, MA60 regime slots 10/5/2, idle cash in 0050, 10 bp slip.
 B: 0050 timing — hold 0050 when TAIEX close > 240-day MA, else cash.
 Not core-satellite: a fixed non-rebalanced blend's CAGR must lie between its parts → cannot beat 0050.
Conclusion: under "earn most, drawdowns count", combo not worth real money now (core or satellite). Return/MDD:
0050 0.84, best combo 0.72. Role: signal / paper only; freeze A and paper-trade 12 months from 2026-10-21. Real
money default 0050; B only if it passes full-period and forward tests. Before anything: rerun with the current code
version and check; measure skipped-signal returns; locate the −52% drawdown period and holdings' size profile.
