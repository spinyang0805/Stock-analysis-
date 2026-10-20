# Stock-personality (股性) — Round P2: cross-examination → ONE merged pre-registration

Read-only, no backtests, no forward returns. Answer in zh-TW (identifiers in English). ≤ 900 words + tables.

Read the four P1 proposals in .omc/plans/debate/: persona_claude_p1.md, persona_sol_p1.md, persona_luna_p1.md,
persona_haiku_p1.md (and the brief .omc/plans/persona-r1-prompt.md).

Facts surfaced in P1 that everyone must take into account:
- research_persona.py (2026-10-19) already tested stock×signal interaction persistence on 8 years → ≈ 0 (CI spans 0).
  This study is therefore a SECOND test of a related hypothesis; thresholds may not be looser than P1 proposals.
- backend/tuning/evaluate.py Costs.slippage defaults to 0.0 → the shared-combo +0.86–1.80%/trade figure may exclude
  slippage; all arms here use 10 bp/side + real fees + tax (stock 0.3%, ETF 0.1%).
- Data: prices/chips 2019–2026 (~2,000–2,500 codes/yr, no 2018), revenue 2018+, SBL 2022+, TDCC only 2025-10+.
  MA240 needs ~1 year warm-up; a 3-year profile window is only complete from 2022.
- Universe is current listings → survivorship bias.
- User hypothesis to respect: "touch rising 月線/季線 + 大戶/法人 enter → surge", profile rebuilt periodically from past data.

## Answer
1. Cross-examine: for EACH other proposal, the 1–2 most serious flaws or strengths (be specific: look-ahead, multiple
   testing, overlap, control design, power).
2. Decide each disputed item (one choice + one-line reason):
   a) situation list (final ≤ 12 base + ≤ 5 combos; name them with indicators.py ids);
   b) combo timing: same-day vs B within w days;
   c) outcome: horizon, benchmark (0050 vs PIT equal-weight pool), entry/exit timing;
   d) persona statistic (stock − pooled same situation, or full interaction θ with stock drift removed) and shrinkage
      (fixed k vs estimated τ);
   e) global gate (Haiku's cross-stock heterogeneity test vs same-day shuffle) — required or not;
   f) membership thresholds (min N, effect size, halves, FDR method/q), re-profile cadence and window, hysteresis;
   g) periods (warm-up / dev / test), and a sealed forward period;
   h) primary pass criteria (event level AND portfolio level), label-shuffle null, and exact variant list (≤ 5);
   i) exit rule in the portfolio test (fixed 20 days vs frozen combo sell);
   j) abandonment conditions.
3. List any remaining BLOCKING doubt, or write "no remaining blocking doubt".
