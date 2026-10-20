# Codex Sol — Round 6 (summary, faithful) — "no remaining blocking doubt"
1. Final: C1 50% 0050 / 50% cash, monthly rebalance. C7 qualifies historically but −34.96% is 0.04 pt from the limit;
   picking the largest weight that just passed on one path overfits 2008. C1's 2.75-pt margin isn't "safe" either —
   decide weight from a stress scenario first (e.g. stocks −70% → 50% stock ≈ −35% static loss). C1 = conservative,
   simple baseline, not proven highest return nor guaranteed ≤35%. C10 acceptable but don't re-search frequency.
2. Re-ran the independent script with python -B and reproduced 13.93/−55.75, 10.74/−33.56, 7.09/−32.25. User message
   (4 sentences) drafted; give the trade-off back to the user; keep C1 until the user changes the risk limit.
3. Frozen forward paper test 2026-10-21 → 2031-10-20 for C1, C8, VT15 (exact rules, data hashes, costs, 2× friction
   case, real cash yield). Pass (all required, judged only at the end): MDD of rule and its registered neighbours ≤35%,
   neighbours keep ≥2/3 of CAGR edge vs C1, not worse than exposure-matched fixed control in both halves, paired 12-month
   block bootstrap vs C1 2.5th pct > 0 (also with 2× friction), at least one 0050 drawdown ≥20% in the window
   (else "insufficient stress evidence"). Breaching 35% voids a pass. Pass only earns a small trial discussion.
4. Cash half: low-risk TWD deposit / short time deposit; 1% is a sensitivity (C1 ≈7.7% with 1%); MMFs: fees,
   redemption timing, credit risk, not deposit-insured (CDIC NT$3M per bank per depositor).
5. Combo & per-stock: 0% real money. If ever run: max 10 names, ≤10% each, shared parameters and rules, fixed
   pre-set ranking when >10 qualify, empty slots stay cash.
6. No blocking doubt for the conservative choice; 35% is not a guarantee; forward tracking still required before
   scaling real money.
