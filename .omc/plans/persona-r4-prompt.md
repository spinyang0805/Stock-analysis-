# Stock-personality (股性) — Round P4: decide after the feasibility count; freeze ONE design

Read-only, no backtests, no returns. zh-TW (identifiers English). ≤ 900 words + tables.
Read your P3 and the others': .omc/plans/debate/persona_{sol,luna,haiku}_p3.md (Claude moderates).

## New facts (count-only, no returns; script scratchpad/persona_feasibility.py, report persona_feasibility.md in
C:\Users\FENC34~1\AppData\Local\Temp\claude\D-------------------Stock-analysis-\1cb0e439-d60f-441d-98bd-c0d5a22f8b2d\scratchpad\)
PIT top-500 universe, 2019-01→2026-10, 1,390 stocks ever in universe, events ≥ 20 sessions apart per stock×situation:
| sit (Sol P3 ids) | events | stocks ≥1 | per stock-yr mean/median | per year range |
|---|---|---|---|---|
| C1 buy main | 11,803 | 1,200 | 2.91 / 3.00 | 582 (2022) – 2,093 |
| C2 trig&money&other&~env | 11,386 | 1,217 | 2.81 / 3.00 | 785 – 2,196 |
| C3 env&trig&money&~other | 1,345 | 707 | 0.42 / 0.14 | 25 – 402 |
| C4 OBVS m1 & state | 642 | 370 | 0.15 / 0 | 33 – 111 |
| C5 OBVS m2 & env & money | 6,612 | 1,105 | 1.68 / 1.70 | 254 – 1,139 |
| C6 OBVS m3 & env & money | 2,724 | 906 | 0.73 / 0.55 | 112 – 477 |
| C7 OBVS m4 & env&trig&money | 565 | 370 | 0.145 / 0 | 28 – 108 |
| C8 OBVS m5 & stage2&trig&money | 542 | 360 | 0.148 / 0 | 48 – 99 |
Eligible stock×situation cells (N ≥ 30 in trailing 36 months, ≥ 15 per half): **0 for every situation at every
month-end 2022-12 → 2026-09** → max labelled events 2023–26 = 0. Per the pre-registered rule this makes formulation (a)
per-stock θ **insufficient** (no loosening allowed).
Also: TPEx industry bug fixed (stock_list.py now reads SecuritiesIndustryCode from TPEx OpenAPI mopsfin_t187ap03_O,
same 2-digit codes as TWSE) → industry strata are available for both markets.
Haiku P3 also reported: per-stock-fitted OBVS was mixed (2 wins / 3 losses across 5 validations); the 8–11 stocks with
the user's 13 CUSTOM OBVS param sets were positive in all 5 (+6.4…+9.3%/trade) but were chosen ex post and lack a
same-stock DEFAULT-param control.

## Decide
1. Record verdict for per-stock (a): insufficient — agree / disagree (one line).
2. Choose the ONE design to freeze now (or "none"):
   (T) TYPE-level persona (Haiku P3): types = volatility tercile × trend/revert (research_persona.traits), rolling 36m,
       per-type choice among exits X1 strict3 / X2 OBVS-style exit (peak −8% & OBV EMA20<60) / X3 fixed 20d on the
       shared entry(s) — or per-type entry choice among C1/C2/C5/C6 — say which, and why;
   (P) pooled composites only (no persona): which composites deserve a shared-rule test;
   (O) a small CLEAN test of the user's 13 CUSTOM OBVS sets vs DEFAULT on the SAME stocks (paired, same dates), as a
       separate, clearly-labelled ex-post-selected experiment (what can and cannot be concluded).
   You may pick T and/or O; justify with power from the counts above.
3. For your choice give the frozen spec delta vs your P3 (thresholds never looser than P2/P3; industry + return-cluster
   strata; type×industry and type×trailing-12m-return checks BEFORE testing; sealed forward 2026-10-21→2027-10-20;
   ≤ 5 variants; abandonment rules).
4. "no remaining blocking doubt" or list BLOCKING items.
