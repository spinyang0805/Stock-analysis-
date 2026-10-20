# Stock-personality (股性) — Round P3: re-scope to COMPOSITE signals (組合拳 + OBVS), then one merged pre-registration

Read-only, no backtests, no forward returns. Answer in zh-TW (identifiers in English). ≤ 1,000 words + tables.

## New instruction from the user (2026-10-20)
"之前的股性研究是單一訊號，現在要改用組合拳的複合訊號 + OBVS 訊號一起看，所以會得到不一樣的研究報告。圓桌一起研究。"
So the persona study must be built on COMPOSITE situations: the 組合拳 layers and the user's OBVS (OBV strategy) signals,
not on the single library signals.

## Prior evidence you must weigh (just retrieved, 8 years 2019-04 → 2026-10, single library signals, 75 buy / 31 sell)
- Persistence of stock×signal interaction between halves (split 2022-12-26): slope −0.02…+0.01, all 95% CIs span 0
  (22,530 pairs, 618 stocks at 20 days).
- Walk-forward yearly pick of 3 signals per stock, next-year 20-day excess vs pool (before ~0.4% round-trip cost):
  pooled (best on all stocks) +1.17% t 11.8; similar-stock group +0.64% t 3.1; shrunk k=30 +0.08% t 0.7;
  own history −0.11% t −0.9.
- Full report: C:\Users\FENC34~1\AppData\Local\Temp\claude\D-------------------Stock-analysis-\1cb0e439-d60f-441d-98bd-c0d5a22f8b2d\scratchpad\p8\research_persona\research_persona.md
Key question: is there a principled reason composite signals could show persistent per-stock differences when single
signals did not (higher signal-to-noise per event? fewer, cleaner events → less power?), and how do we test it without
loosening thresholds (this is now the THIRD look at a persona hypothesis on the same data)?

## Material
- 組合拳: backend/tuning/research_combo.py `masks(g, detail=True)` — layers env (stage2 & market regime ≠ −1),
  stage2, stage4, trig (TRIGGER breakout within 3 d), money (MONEY or OBV EMA20/60 golden cross within 5 d),
  other (CHIPS/RS within 10 d or REV within 40 d), buy states main/no_env/no_other/trigger_only, sell layers brk, conf,
  n_struct, n_conf, strict-B sell (strict3). Read the TRIGGER/MONEY/CHIPS/RS/REV lists in the file.
- OBVS: backend/tuning/obvs.py — modes 1–5 (OBV EMA fast/slow hold; OBV N-high/M-low channel; RSI(OBV) 55/45;
  price turn up pu% / turn down pd% with OBV fast<slow; mode 4 + OBV golden cross), DEFAULT params and the script's 13
  per-stock CUSTOM sets (the user's own "per-stock personality" idea in OBVS form).
- Earlier OBVS research results (research_single families obvs / obvs_fixed): per-stock fitted OBVS sets did not beat
  0050 out of sample (see memory summary in .omc/plans/combo-strategy-brief.md if present).
- Your previous P1/P2 answers and the others' in .omc/plans/debate/persona_*_p1.md and persona_*_p2.md.
- Data facts: TPEx chips 2019–21 checked OK (repeat-value rate 2–3%, same as fixed 2022). Industry codes exist for TWSE
  only; TPEx (~900 stocks) has no industry → industry-stratified shuffle needs TPEx industry fetch or a return-
  correlation cluster proxy. Survivorship: current listings only. Costs: evaluate.Costs slippage default 0 → pass
  slippage=0.001 explicitly. Revenue usable from the 11th (fetch_extra.py).

## Answer
1. Re-scope: list the composite situations (≤ 8) built ONLY from 組合拳 layers and OBVS modes (e.g. combo main buy;
   combo trigger with money but not chips; OBVS mode-5 entry inside stage2; OBVS channel breakout + combo money; ...).
   Exact definitions with existing function/field names; expected events per stock per year.
2. What is the "personality" here? Choose ONE primary formulation, e.g.
   (a) per-stock response to each composite (interaction θ, as before), or
   (b) per-stock choice among OBVS parameter sets / modes (like the user's 13 CUSTOM sets) on top of the combo filter,
   or (c) per-stock exit style (strict-B vs OBVS mode exit vs fixed 20 d).
   Justify with theory and power; say which formulation the prior evidence makes least/most likely to work.
3. Carry over the P2 decisions that still apply (control arm = same composite on all stocks, label shuffle, sealed
   forward 2026-10-21→2027-10-20, warm-up/dev/test periods, account-level comparison vs 0050 and shared combo, costs,
   N/q thresholds — never looser than P2) and state any changes forced by composites (lower event counts!).
   Include a feasibility rule: count events only (no returns) before freezing N; if too few → report "insufficient",
   no loosening.
4. Exact variant list (≤ 5) and abandonment conditions.
5. Remaining BLOCKING doubt, or "no remaining blocking doubt".
