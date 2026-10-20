# Pre-registration — round 4 allocation test (written BEFORE running, 2026-10-20)

User constraint: max drawdown ≈ 35%; within it, highest long-run return. User instruction: avoid overfitting.

## Anti-overfitting rules (fixed now, apply to every candidate)
1. Candidates are only those proposed by the round-table models in round 4, with their stated parameters. No parameter
   is tuned after seeing results. Every tested variant is listed in the report, including failures.
2. Primary sample: 2004-01-02 → 2026-10-19 (includes 2008). A candidate qualifies only if its MDD ≤ 35% over the full
   period AND it is not worse than the simple fixed-mix control of the same average exposure in BOTH sub-periods
   (2004–2014 and 2015–2026).
3. Stability: rules with a length/threshold must hold up for the neighbouring values the proposer named (no picking the
   best neighbour); if only one value works, it is rejected as fragile.
4. If the best qualifier's CAGR edge over the simplest qualifier is within the block-bootstrap 5–95% band, choose the
   SIMPLER rule.
5. Synthetic 2× before 2014-10 is flagged; a candidate relying on leverage must also qualify on 2015+ with real 00631L.
6. Whatever is chosen is frozen and must still pass forward paper tracking before real-money size increases.

## Registered candidates (Sol, round 4) — more will be appended from Luna/Haiku round 4 BEFORE the run
- C1 fixed 50% 0050 / 50% cash, decided month-end, rebalanced next month's first open
- C2 fixed 60% 0050 / 40% cash, same schedule
- C3 Rule A volatility target: weekly, σ = 60-day stdev of daily 0050 returns × √252, E = min(1.5, 0.15/σ);
     E ≤ 1 → E in 0050; 1 < E ≤ 1.5 → (2−E) 0050 + (E−1) 00631L (synthetic 2× before listing)
- C4 Rule B drawdown budget (CPPI): E = min[1.5, max(0, 3(V − 0.70H)/V)] daily, start 0.9, cash forever once E = 0
- Controls already run: 0050 B&H, MA200/240/280 timing variants (0050, 50/50, 2×), 50/50 and 2× B&H.

## Appended from Haiku round 4 (before the run)
- C5 R1 vol target: E = min(1, 0.20/σ21), σ21 = 21-day stdev of 0050 daily returns × √252, weekly, 0050 or cash
- C6 R2 MA200 no-trade band: in when TAIEX > 1.02×MA200, out when < 0.98×MA200, else hold state; 0050 or cash
- C7 fixed 55% 0050 / 45% cash monthly (NOTE: chosen by Haiku from a 50–65% grid — disclosed as a grid pick; C1 50%
     and C2 60% are its registered neighbours, so C7 is only accepted if C1 also qualifies)
- Data freeze: market_index.parquet as of 2026-10-20 09:32 (2002-01-02 … 2026-10-19, 6,104 rows); report its hash.
- Report starts 2004-01-02, 2005-01-03 and 2006-01-02 for every candidate (no picking the best start).

## Appended from Luna round 4 (before the run)
- C8 drawdown trigger: H = highest adjusted 0050 close since the start; while invested, close ≤ 0.90·H → next open
     40% 0050 / 60% cash; while reduced, close ≥ 0.95·H → next open 100% 0050; else keep state
- C9 50% 0050 / 50% cash, NO rebalancing after the initial purchase (dividends stay in 0050)
- C10 50% 0050 / 50% cash, rebalanced quarterly (decided at the quarter's last close, traded next open)
- Sensitivity for ALL candidates (registered now): cash earning 1%/yr instead of 0%.

## Robustness checks (registered after the main run, BEFORE looking at them; for rejection only, never for picking)
- C6 neighbours: band ±1% and ±3% (MA200); MA180 and MA240 with ±2%. C6 is rejected as fragile if any neighbour
  breaks MDD ≤ 35% or loses more than a third of C6's CAGR edge over C1.
- C8 neighbours: cut 0.88/0.92, low 0.30/0.50.
- Exposure-matched controls: for each timing rule, a fixed monthly mix at that rule's average exposure.
- C3 neighbours (registered before viewing): 0050-only (max 1.0) variant with target 0.13 / 0.17 and lookback 40 / 90,
  same rejection rule as C6. NOTE (disclosed): "C3 0050-only" was added after the main run as the simpler form of C3;
  pre-registered rule 4 (prefer simpler) applies, but it counts as an extra variant.
- Disclosed framing issue: rule 2 compares with a fixed mix of the same average exposure, but such mixes (72% → MDD −44%,
  90% → −52%) violate the user's 35% limit, so they are not admissible alternatives; the decision compares admissible
  candidates (MDD ≤ 35%) only. Rule 2 results are still reported.
- Round 5 (Sol): VT15 REJECTED by the registered neighbour rule (VT17 MDD −36.45% > 35%), same as C6. Rule 4 test for C8:
  paired 12-month block bootstrap of daily log-return difference C8 − C1 (simplest admissible), 2000 draws, seed 7;
  C8 accepted over C1 only if the 5th percentile of the annualised difference > 0.
- Round 5 (Luna, accepted): rule 2 may not be dropped after seeing results. Read strictly on CAGR in BOTH sub-periods vs
  the exposure-matched fixed mix: C8 fails 2004–14 (3.5% vs fixed-72% 5.3%); VT15/C3 fail 2015+ (14.7% vs fixed-90%
  18.6%). → No dynamic rule passes the registered screens. Remaining admissible: fixed mixes C1/C7/C10 (C9 drifts).
  Rule 4 test C7 − C1 (paired bootstrap, same settings) decides between them.
- USER DECISION (2026-10-20): the 35% drawdown limit is judged against a 2008-type crash (not 2000–01 or 1990).
  → stock weight ≈ 50% (C1: 2004+ 7.2%/−32.2%; TAIEX-price 2008 sim 50% −34.1%). Disclosed: a 2000–01-type crash
  (−66%) would take a 50% mix to about −40%.
- USER WAIVER (2026-10-20, in chat: 「同意例外」): rule 6 is waived for the STATIC allocation only (C10, 50% 0050 / 50%
  cash, quarterly), after being shown: 2008 path −34.2% (0.8 pt margin), 2000–01 path −37%, 2008×1.2 stress −36…−39.5%.
  The waiver is post-hoc and disclosed as such. Dynamic rules (C8, VT15, combo, per-stock) are NOT waived. User then asked
  why 50% — final weight pending their confirmation (50% vs 40%).
- USER DECISION (2026-10-20): weight = 50% (chose "higher return" over 40% after seeing the 2008×1.2 breach). FINAL real-money
  policy: C10 = 50% 0050 / 50% TWD cash (insured deposit), decided at each quarter's last close, traded next session's open.
