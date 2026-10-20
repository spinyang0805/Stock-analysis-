# Haiku 5.5 (max) — persona P1 (faithful summary)
r20x = t+1 open → t+21 open, after cost, minus PIT top-500 equal-weight. 法人 = 外資+投信. edge() events.
A. Base 11: touch_ma20, touch_ma60, touch_ma240 (pooled only), brk_20, rs_high, vol_20_up, bb_squeeze_brk, chip_p80,
foreign_streak_3, trust_first, rev_high12. Combos (A today, B within w days): C1 touch_ma60∧chip_p80 w5; C2 touch_ma20∧rs_high
w10; C3 touch_ma60∧trust_first w10; C4 brk_20∧chip_p80 w5; C5 brk_20∧vol_20_up w3; C6 touch_ma60∧rev_high12 w40.
Dedup'd events ~5–12/yr/stock for MA20 (not 15–30).
B. Monthly rebuild, matured events only (event+21 ≤ rebuild), 750-day window (+ all-history sensitivity). θ = stock×situation
interaction (same as research_persona.py), N ≥ 8. GLOBAL GATE: per situation cross-stock heterogeneity Q test vs same-day
shuffle null (1,000×), Holm α .05; failed situations cannot type stocks. Shrink θ̃ = τ²/(τ²+SE²)θ (τ by method of moments).
Family score ≥ +1.0%, abs excess > 0, both halves > 0; type changes only after 2 consistent months. Types: 1 MA bounce,
2 breakout, 3 chips, 4 revenue, 5 false-breakout (avoid), 6 none (expected majority; if any type > half of stocks → too loose).
Exit = combo sell rule frozen.
C. Data: 2019–26 ~2,000–2,500 codes/yr, no 2018 → 2019–20 warm-up, dev 2021–22, test 2023–26. COST WARNING: research_combo
Costs() default slippage 0 → +0.86–1.80%/trade may exclude slippage; recompute with 0.1%/side (round trip ≈ 0.67%+).
Event test: (a) persona vs (b) same situations all stocks vs (c) 1,000 label shuffles; pass all: diff ≥ +0.5%/trade & t ≥ 2;
dev diff > 0; (a) net excess > 0; p vs shuffle < .05; cross-stock rank corr of θ (half vs next 12m) ρ ≥ 0.10 with CI > 0.
Portfolio: ≤10 names equal, combo exits; must beat 0050 AND combo CAGR in test, MDD ≤ 0050 + 10pp. 5 variants disclosed.
D. Risks: (1) persona too weak to estimate (SE 2–4% per stock-situation; ρ ≈ τ²/(τ²+SE²)); abandon if no situation passes
gate or ρ < 0.10. (2) persona = β/trend/industry/regime alias; abandon if gone after controls or regime signs differ.
(3) data reused many times; research_persona.py (2026-10-19) already tested this hypothesis (persistence ≈ 0) → this is a
SECOND test, thresholds can't loosen; survivorship bias. Forward: freeze, log from 2026-10-21, abandon 2027-10 if not better.
