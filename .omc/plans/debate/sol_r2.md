# Codex Sol — Round 2 (summary, faithful)
1. Verdict: the original combo is NOT worth real money now.
 - Luna: "per-trade ≠ compounding" confirmed; rank priority 20.3%→14.3% so not a priority improvement (hard top-100
   filter untested). ATR+risk sizing 11.1% refutes "less risk per trade compounds better" (tested 3ATR clipped).
 - Luna's dismissal of bear fast-exit needs correction: CAGR 26.9% (up) but MDD −48%, 2022 −26%→−32%; not proven to
   protect capital; category-based counting untested.
 - Haiku: account-level verdict rule fully right; idle 0050 helps. But "about equal to market" too optimistic: base
   lags 8.2 pts CAGR and 18 pts MDD; zero-slippage still 21.5% → slippage is not the main cause.
 - Haiku's exposure idea partly right: MA60 slot caps work; MA240 caps 17.7%/−48% do not.
 - Opus: account-level move right; NaN fix necessary.
 - Sol withdraws round-1 optimism on ATR sizing, profit trail, rank priority. Keeps MA60 slot caps as a candidate.
2. Why it lags 0050: gap is 2024/2025/2026 (+15/−19/+20 vs +49/+37/+70). Small/mid-cap tilt vs TSMC-led index is a
   plausible hypothesis, not yet attributed. 27% win → depends on few big winners. Slot competition: RS20 order, skipped
   gap-ups are not back-filled (research_portfolio.py:175). "Exposure cap" only gates NEW entries (10/5/2 slots, not
   exact 25%). Trail threshold can move down when ATR grows (not ratcheting). Execution idealised: no T+2 cash ledger,
   no limit-down sell check; 0050 parking has no slippage and the B&H benchmark has no entry costs; no 500-day CAP in
   simulate(); trade stats are closed trades only.
3. Three pre-registered finalists (NT$10M, no leverage, current buy mask, strict B, −10% stop, 500-day cap, RS20 order,
   10 bp slippage on ALL trades, T+2 cash, consistent total-return accounting, end-of-period liquidation cost):
   甲 100% 0050 buy & hold (dividends reinvested) — default and benchmark.
   乙 initial 80% 0050 + 20% combo, separate books, no rebalancing; combo max 10, 10% of its book each, MA60 regime
      new-entry slots 10/5/2, idle cash stays cash.
   丙 whole account with 10/5/2 slots, idle cash in 0050 (sell 0050 to fund buys).
   Pass for 乙/丙: full-period CAGR > 甲 AND |MDD| ≤ 甲. Otherwise rejected. Survivors still need frozen-rule forward
   paper trading 2026-10-20 → 2029-10-20, both criteria again.
4. Real-money role now: signal observation / paper trading only; default allocation 甲. Combo could become a 20%
   satellite only after passing 乙 fully.
