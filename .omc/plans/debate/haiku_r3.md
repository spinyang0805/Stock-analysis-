# Claude Haiku (max) — Round 3 (summary, faithful)
Code/data check: limit-down deferral, 0050 parking slippage, B&H costs, liquidation, cap, liquidity cap (only 4 skips),
no-buy-without-market-data all confirmed; no look-ahead found (revenue as-of 11th, TDCC strictly earlier, no shift(-)).
Bugs: (a) T+2 equity omitted unsettled cash (A_T2 MDD fake) [fixed]; (b) etf_timing logs 0.0 trades → "14 trades,
0% win" meaningless (14 = exits); (c) fast exit counts warning-only s_macd_down/s_ma20_break; (d) Haiku r2's 3.1×/1.1×
figures were A_regime, base_N10 is 3.7×/0.94× (direction unchanged).
Survives: combo not for real money (all MC medians < 0050; P(both) ≤12%). Regime caps improve base but are mixed with
idle-0050 — not isolated. Rank/trail/ATR/MA240-caps results are single-path, mostly old simulator → "unverified", not
"refuted". Validation half-year not out-of-sample (12-minute commits). Remaining biases (survivorship, no delisting
recovery, T+0 reuse) favour combo → "combo loses to 0050" is a conservative conclusion.
Q1 stop: no best % (medians 21–26%, within 12–26-pt bands); MDD medians all −37/−38% → the stop doesn't cut drawdowns;
it cuts later-recovering trades. If kept: 10%, pre-registered, as a per-stock tail cap.
Q2: logically never stuck (stop + stage-4 are independent price exits); trading-level risks: suspensions, one-price
limit-downs, delisting (no delisting events in data). Stress: extra −3% slippage on stop exits; 20 days without a fill
→ liquidate at 0.5 × last close; rerun MC.
Q3: per-stock personality — settled NO. Pooled ML — not now (≈7 market years, low power); later only with ≤5
pre-registered features, yearly walk-forward with 60-day purge, ≥5 of 6 OOS years positive, must beat no-model.
Remaining doubts / decisive tests: D1 T+2 rerun; D2 isolate regime caps (N10_idle0050 vs A, 40 MC each); D3 start-date
scan; D4 BIGGEST: 0050-timing B in 2000–2017 (2008 ≈ −60%, 2011, 2015) — only one real bear (2022) in sample;
D5 leverage only MDD-matched, with lower-return scenarios; D6 gap/delisting stress; D7 strict-B alias de-dup;
D8 cross-sectional value of RS ordering; D9 2024–26 attribution (combo vs equal-weight top500 vs 0050).
Real money: combo none (incl. 80/20). Default 0050 B&H. Only formal risk-managed candidate: B (0050 above 240-MA else
cash): price-only 22.5%/−21.7%, Calmar 1.21 vs 0.84; MA150–260 all −22/−23% MDD. Leverage estimates (price-only, 5.5%
financing, 1% fee): B×1.4 ≈ +29%/−30%; B×1.6 ≈ +32%/−33%; 2× ≈ +38%/−40%. Leverage doesn't improve Calmar.
DECISION RULE: the user must set a max-drawdown limit. −34%: 0050 wins; −25%: only B; −30%: B×1.4. "CAGR > 0050 AND
MDD not deeper" always fails for any risk-managed plan — stop using it. Timeline: 0050 default until D1–D4 pass;
B / MDD-matched leverage only after D4 (2008) passes, then 12 months frozen paper trading; no margin for real money yet.
