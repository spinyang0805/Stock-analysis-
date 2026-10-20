# Codex Sol — persona P4 (faithful summary)
(a) per-stock: insufficient — agree; stop, no loosening/merging/window extension.
Freeze ONLY T: shared entry C1 = buy["main"], per-TYPE exit choice. No entry choice, no O (CUSTOM ex-post + no DEFAULT control).
Types: research_persona.traits volatility tercile × trend/revert (6), month-end update on prior 36m, effective next month.
Exits: X1 strict3 + 10% stop (historical version excludes SBL/TDCC whale fields — list the diff first); X2 close ≤ peak close
×0.92 AND OBV EMA20<EMA60; X3 t+1 open → t+21 open. Signal exits at next fillable open; no TP/extra stops.
Selection: trailing-36m completed paired events; type choice must beat same-window pooled best exit AND fixed X1 by ≥1pp net,
both halves >0, corrected CI >0, else X1; ties X1→X3→X2; 3 months to switch, first failure back to X1.
Samples: per type per window ≥200 events, ≥20 stocks, each 18m half ≥100; main eval ≥500 & ≥20 stocks; deduped completed
events, no triple counting across exits. Global gate 1,000 full-rerun shuffles, Holm .05; BY q ≤.05 over 6 types × 2 controls;
stock+60d block inference; persistence ρ ≥.10 CI>0; drop-top robust.
Pre-test (no strategy returns): type×industry and type×trailing-12m-return quintile crosstabs + stratum counts; industry codes
both markets + PIT return-correlation clusters; shuffle within date×situation×regime×exchange×industry×cluster; non-exchangeable
→ insufficient; no post-hoc strata merging; if type can't be separated from industry/momentum → no persona claim.
Periods: 2019 warm-up; 2020–22 dev; 2023 pipeline; 2024 & 2025–2026-10-20 audit; ONLY confirmation = sealed 2026-10-21→2027-10-20.
Arms: T, pooled exit, fixed X1, fixed X3, P3 combo, 0050 TR; NT$1M, ≤10, ≤10%, round lots, NT$20 min fee, T+2, liquidity,
non-fillable; Costs(slippage=0.001); 200 seeds median CAGR beats all, MDD ≤ 0050. Variants V0, V1 (15bp) only.
Abandon on insufficient / any gate / half / persistence / drop-top / strata / account failure / type churn > 1/3.
BLOCKING: type & stratum count-only feasibility + confound checks; C1/X1 data vintage + halt/limit rules; X2 peak-update /
next-fill semantics; delisted coverage of historical universe.
