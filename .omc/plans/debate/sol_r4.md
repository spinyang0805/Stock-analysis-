# Codex Sol — Round 4 (summary, faithful)
1. Withdraws round-3 "real money 100% 0050": 0050's 2004+ MDD −56% violates the user's ~35% limit. Not final.
2. Both periods matter: 2004+ tests survival, 2015+ tests recent relevance; TSMC concentration may change returns but
   doesn't rule out range markets / deep bears (concentration can amplify common drops). Overlapping samples; can't
   pick the friendliest start.
3. MA timing's recent gain is real history, not a persistent edge: 50/50 MA200 36.2%/30% (2020+), 26.0%/30% (2015+),
   12.4%/49% (2004+). Correction: pure 2× MA200 15.5% > 0050 13.8% but MDD 59%. 0050 MA200 2015+ 18.0% < hold 20.9% →
   the 50/50 gain is leverage, not timing; need same-weight rebalanced HOLD controls.
4. Two frozen dynamic rules (test once, no search). Exposure E: E≤1 → E in 0050, rest cash; 1<E≤1.5 → 0050 weight
   2−E, 00631L weight E−1.
   Rule A vol target: every week's last close, σ = stdev of last 60 daily 0050 total returns × √252;
   E = min(1.5, 0.15/σ); trade next open; no data → cash.
   Rule B drawdown budget (CPPI): daily V (account), H (high-water incl. initial capital);
   E = min[1.5, max(0, 3(V − 0.70H)/V)], next open; start E 0.9; at 0 stays in cash (0% interest).
5. Fixed controls: 0050/cash 50/50 and 60/40, decided at month-end close, rebalanced next month's first open.
   Compare 2004+, 2004–2014, 2015+: CAGR, MDD, recovery time, worst year. Filter MDD ≤ 35% over the full period, then
   pick highest CAGR; if within uncertainty, prefer the simple one.
6. Combo: 0% real money; paper/signal only; pooled ML only as separate research.
7. Doubts / tests: weight simulator trades 0050 before the 2× leg (switch back may lack cash) → sells first, buys
   second, conservation test [moderator: fixed]; weight_rule decides on the period's FIRST day (trade on the 2nd) →
   use period END [moderator: fixed]; run() 50/50 is set at entry, not rebalanced → also test monthly-rebalanced; check
   the synthetic 2× against real 00631L tracking; cost stress; contiguous sub-periods and block resampling; gaps.
8. Interim real money: 50% 0050 + 50% cash, 0% 00631L, 0% combo, month-end rebalancing — not yet proven ≤35%.
