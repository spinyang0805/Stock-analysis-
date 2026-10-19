"""Independent per-stock signal-to-signal research; writes research_codex.md only.

python backend/tuning/research_codex.py --source parquet --universe 500 --years 8 --adjust-source exchange --workers 4

Shared prepare() retains its usual data-cache behavior. No deployment/DB writes.
"""
import argparse
import math
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning.run_tuning import prepare, add_common_args
from tuning import indicators as ind
from tuning.evaluate import Costs, t_cluster, CLUSTER_DAYS
from tuning.data import OUT_DIR, log

CAP, STOP, TRAIL = 250, 0.08, 0.12
VARIANTS = ("signal_only", "stop_trail")
LABEL = {"signal_only": "僅賣訊（無額外停損）", "stop_trail": "賣訊＋8%停損＋12%移動停利"}
TRADE_COLUMNS = ["stock_id", "variant", "sample", "fold", "buy", "sell",
                 "p", "entry", "exit", "net", "bench", "excess", "days", "reason"]


@dataclass(frozen=True)
class Config:
    train_days: int = 756
    min_train_days: int = 504
    test_days: int = 126
    holdout_days: int = 126
    rebalance_days: int = 21
    turnover_lookback: int = 60
    shortlist_buys: int = 4
    finalists: int = 3
    min_discovery: int = 4
    min_confirmation: int = 3
    shrink: float = 8.0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=("parquet", "db"), required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--train-days", type=int, default=756)
    ap.add_argument("--min-train-days", type=int, default=504)
    ap.add_argument("--test-days", type=int, default=126)
    ap.add_argument("--rebalance-days", type=int, default=21)
    ap.add_argument("--fee-discount", type=float, default=0.6)
    ap.add_argument("--slippage", type=float, default=0.0, help="adverse fraction per side")
    ap.add_argument("--end-date", default=None, help="YYYYMMDD historical as-of date")
    add_common_args(ap)
    ap.set_defaults(no_holdings=True, out_dir=OUT_DIR)
    a = ap.parse_args(argv)
    if (a.workers < 1 or a.universe < 1 or a.years <= 0 or a.test_days < 1
            or a.min_train_days < 252 or a.train_days < a.min_train_days
            or a.rebalance_days < 1 or a.slippage < 0 or not 0 <= a.fee_discount <= 1):
        ap.error("invalid workers/universe/years/window/cost settings; min-train-days >= 252")
    return a


def make_folds(n, cfg):
    """All bounds are global exchange-session positions; hi is exclusive."""
    h = n - cfg.holdout_days
    if h <= cfg.min_train_days:
        raise ValueError("Insufficient history for walk-forward plus untouched 126-session holdout")
    folds = [(max(0, t - cfg.train_days), t, min(t + cfg.test_days, h), "walk_forward", k)
             for k, t in enumerate(range(cfg.min_train_days, h, cfg.test_days))]
    folds.append((max(0, h - cfg.train_days), h, n, "holdout", len(folds)))
    return folds


def membership_arrays(prep, calendar):
    pos = {str(d): i for i, d in enumerate(calendar)}
    members = {str(s): np.zeros(len(calendar), bool) for s in prep["adjusted"]}
    if prep.get("inu") is None:
        raise ValueError("Point-in-time membership is required")
    for sid, g in prep["inu"].groupby("stock_id", sort=False):
        if str(sid) in members:
            ix = np.array([pos[str(d)] for d in g["date"] if str(d) in pos], int)
            members[str(sid)][ix] = True
    return members


def benchmark_index(adjusted, calendar, members):
    """Gross daily EW, open p -> p+1, members known at close p-1.

    Suspended traded opens carry forward; zero until a real open, then catch up.
    Members without an initial price earn zero, not winner-only renormalization.
    """
    n = len(calendar)
    total, count = np.zeros(n - 1), np.zeros(n - 1, int)
    stale = 0
    for sid, g in adjusted.items():
        gg = g.assign(date=g["date"].astype(str)).set_index("date").reindex(calendar)
        valid = gg.get("fill_ok", gg["volume"] > 0).fillna(False).to_numpy(bool)
        op = gg["aopen"].to_numpy(float)
        valid = valid & np.isfinite(op) & (op > 0)
        mark = pd.Series(np.where(valid, op, np.nan)).ffill().to_numpy()
        lag_member = np.r_[False, members[str(sid)][:-1]][:-1]
        good = np.isfinite(mark[:-1]) & np.isfinite(mark[1:]) & (mark[:-1] > 0)
        ret = np.divide(mark[1:], mark[:-1], out=np.ones(n - 1), where=good) - 1
        total += np.where(lag_member, ret, 0)
        count += lag_member
        stale += int(np.sum(lag_member & (~valid[:-1] | ~valid[1:])))
    r = np.divide(total, count, out=np.zeros(n - 1), where=count > 0)
    if np.any(~np.isfinite(r)) or np.any(r <= -1):
        raise ValueError("Invalid equal-weight benchmark returns")
    return np.r_[1.0, np.cumprod(1 + r)], {"stale": stale, "member_intervals": int(count.sum())}


def next_index(mask):
    """First true index >= i, sentinel n; reverse cumulative minimum."""
    n = len(mask)
    return np.r_[np.minimum.accumulate(np.where(mask, np.arange(n), n)[::-1])[::-1], n]


class StockEngine:
    """Precompute exits once; each rule simulation visits trades, not daily bars."""
    def __init__(self, g, calendar, member, bench, costs):
        self.n = n = len(calendar)
        self.bench, self.costs = np.asarray(bench, float), costs
        g = g.sort_values("date").reset_index(drop=True).copy()
        g["date"] = g["date"].astype(str)
        positions = pd.Index(calendar).get_indexer(g["date"])
        if np.any(positions < 0) or len(set(positions)) != len(positions):
            raise ValueError("Stock dates must be unique and in calendar")
        f = ind.features(g)
        buys, sells = ind.buy_signals(f), ind.sell_signals(f)
        self.op, self.close = np.full(n, np.nan), np.full(n, np.nan)
        self.fill = np.zeros(n, bool)
        self.op[positions] = g["aopen"].to_numpy(float)
        self.close[positions] = g["aclose"].to_numpy(float)
        raw_fill = (g["fill_ok"].fillna(False).to_numpy(bool) if "fill_ok" in g
                    else g["volume"].fillna(0).to_numpy() > 0)
        self.fill[positions] = raw_fill
        self.fill &= np.isfinite(self.op) & (self.op > 0)
        self.mark = pd.Series(np.where(self.fill, self.op, np.nan)).ffill().to_numpy()
        self.next_fill = next_index(self.fill)
        self.entries = {}
        for name, signal in sorted(buys.items()):
            p = positions[signal.fillna(False).to_numpy(bool)]
            p = p[p + 1 < n]
            e = p + 1
            valid = (member[p] & self.fill[e] & np.isfinite(self.close[p])
                     & (self.close[p] > 0) & (self.op[e] < self.close[p] * 1.095))
            self.entries[name] = p[valid]
        ar = np.arange(n)
        cap_decision = np.minimum(ar + CAP - 1, n)
        stop_decision = np.full(n, n, int)
        peak = self.op.copy()
        # 250 vector operations over every possible entry, not pairs x days.
        for offset in range(min(CAP, n)):
            e = ar[:n - offset]
            c = self.close[offset:]
            peak[e] = np.fmax(peak[e], c)
            hit = ((c <= self.op[e] * (1 - STOP)) | (c <= peak[e] * (1 - TRAIL)))
            first = hit & (stop_decision[e] == n) & self.fill[e] & np.isfinite(c)
            stop_decision[e[first]] = e[first] + offset
        self.exits = {}
        for name, signal in sorted(sells.items()):
            mask = np.zeros(n, bool)
            mask[positions] = signal.fillna(False).to_numpy(bool)
            sell_decision = next_index(mask)[:n]
            for variant in VARIANTS:
                decision = np.minimum(sell_decision, cap_decision)
                reason = np.where(cap_decision <= sell_decision, "cap", "signal")
                if variant == "stop_trail":
                    hit = stop_decision < decision
                    decision = np.minimum(decision, stop_decision)
                    reason = np.where(hit, "stop/trail", reason)
                xp = self.next_fill[np.minimum(decision + 1, n)]
                self.exits[(variant, name)] = (xp, reason)
        self.sell_names = sorted(sells)

    def simulate(self, buy, sell, variant, lo, hi, busy=-1, mark_terminal=False):
        """Training labels cannot see hi or later. Pending trades are marked.

        Terminal marks include round-trip costs but not completed-trade evidence.
        Exit-day close signals are skipped to match the tracker's cooldown.
        """
        signals = self.entries[buy]
        xp, reasons = self.exits[(variant, sell)]
        j = int(np.searchsorted(signals, max(lo, busy + 1)))
        rows, opened = [], None
        while j < len(signals):
            p = int(signals[j])
            e = p + 1
            if p >= hi or e >= hi:
                break
            x = int(xp[e])
            if x >= hi:
                opened = (p, e, sell, buy)
                if mark_terminal and np.isfinite(self.mark[hi - 1]):
                    rows.append(self.outcome(p, e, hi - 1, self.mark[hi - 1], "mark"))
                busy = self.n
                break
            rows.append(self.outcome(p, e, x, self.op[x], str(reasons[e])))
            busy = x
            j = int(np.searchsorted(signals, x + 1))
        return rows, busy, opened

    def outcome(self, p, e, x, price, reason):
        net = float(self.costs.net(price / self.op[e] - 1, False))
        b = float(self.bench[x] / self.bench[e] - 1)
        return (p, e, x, net, b, net - b, x - e, reason)


def evidence(rows, minimum, shrink, z=1.0):
    """Shrink to no edge; variance floor discourages tiny lucky samples."""
    if sum(r[-1] != "mark" for r in rows) < minimum:
        return -math.inf
    x = np.array([r[5] for r in rows])
    if len(x) < 2 or not np.isfinite(x).all():
        return -math.inf
    se = max(float(x.std(ddof=1)), 0.02) / math.sqrt(len(x))
    return len(x) / (len(x) + shrink) * float(x.mean()) - z * se


def choose_rule(engine, variant, lo, hi, cfg):
    """Discovery screens buys then pairs; only 3 pairs see confirmation.

    Future exit arrays are caches: outcomes >=hi are NEVER used as labels.
    Marks at hi-1 prevent dropping unresolved losers from training scores.
    """
    split = lo + (hi - lo) * 2 // 3
    reference = "s_ma20_break"
    if reference not in engine.sell_names:
        return None

    def evaluate(buy, sell, start, end, minimum, z=1.0):
        rows, _, _ = engine.simulate(buy, sell, variant, start, end, mark_terminal=True)
        return evidence(rows, minimum, cfg.shrink, z)

    screened = [(evaluate(b, reference, lo, split, cfg.min_discovery), b) for b in engine.entries]
    screened.sort(key=lambda x: (-x[0], x[1]))
    buys = [b for score, b in screened[:cfg.shortlist_buys] if np.isfinite(score)]
    pairs = [(evaluate(b, s, lo, split, cfg.min_discovery), b, s)
             for b in buys for s in engine.sell_names]
    pairs.sort(key=lambda x: (-x[0], x[1], x[2]))
    finalists = [(b, s) for score, b, s in pairs if score > 0][:cfg.finalists]
    # Fixed family-size penalty, NOT a calibrated p-value or FDR claim.
    z = math.sqrt(2 * math.log(cfg.finalists / 0.10))
    confirmed = [(evaluate(b, s, split, hi, cfg.min_confirmation, z), b, s) for b, s in finalists]
    confirmed.sort(key=lambda x: (-x[0], x[1], x[2]))
    return (confirmed[0][1], confirmed[0][2]) if confirmed and confirmed[0][0] > 0 else None


def research_stock(sid, g, calendar, member, bench, costs=None, cfg=None):
    """Per-stock API for real/synthetic frames. Returns small OOS IPC payloads.

    WF carries existing positions/rules across folds. Holdout starts flat and
    has one frozen rule per variant, fitted strictly before its first session.
    """
    cfg, costs = cfg or Config(), costs or Costs()
    engine = StockEngine(g, calendar, np.asarray(member, bool), bench, costs)
    folds = make_folds(len(calendar), cfg)
    trades, selected, terminal = [], [], []
    for variant in VARIANTS:
        rules = [choose_rule(engine, variant, lo, start, cfg) for lo, start, _, _, _ in folds]
        for sample in ("walk_forward", "holdout"):
            subset = [(fd, rule) for fd, rule in zip(folds, rules) if fd[3] == sample]
            end, busy = subset[-1][0][2], -1
            for (lo, start, stop, _, k), rule in subset:
                selected.append((str(sid), variant, sample, k, start, *(rule or ("現金", "現金"))))
                if rule is None or busy >= stop:
                    continue
                b, s = rule
                signals = engine.entries[b]
                xp, reasons = engine.exits[(variant, s)]
                j = int(np.searchsorted(signals, max(start, busy + 1)))
                while j < len(signals):
                    p = int(signals[j])
                    e = p + 1
                    if p >= stop or e >= end:
                        break
                    x = int(xp[e])
                    if x >= end:
                        mark = engine.mark[end - 1]
                        if np.isfinite(mark):
                            r = engine.outcome(p, e, end - 1, mark, "mark")
                            terminal.append((str(sid), variant, sample, k, b, s, *r))
                        busy = engine.n
                        break
                    r = engine.outcome(p, e, x, engine.op[x], str(reasons[e]))
                    trades.append((str(sid), variant, sample, k, b, s, *r))
                    busy = x
                    j = int(np.searchsorted(signals, x + 1))
    return trades, selected, terminal


def _stock_job(task):
    return research_stock(*task)


def metrics(rows):
    if rows.empty:
        return ["0"] + ["—"] * 10
    x = rows["excess"].to_numpy(float)
    t, clusters = t_cluster(x, rows["p"].to_numpy(int) // CLUSTER_DAYS)
    pct = lambda v: f"{v * 100:.3f}%"
    days = float(rows["days"].mean())
    return [str(len(rows)), pct(x.mean()), pct((x > 0).mean()), f"{t:.3f}（{clusters} 群）",
            pct(rows["net"].mean()), f"{days:.1f}",
            pct(100 * x.sum() / rows["days"].sum()) if rows["days"].sum() > 0 else "—",
            pct(rows["net"].min()), pct((rows["reason"] == "cap").mean()),
            pct(x.min()), str(int((rows["days"] > CAP).sum()))]


def build_report(trades, selected, terminal, calendar, cfg, args, diagnostics, elapsed, coverage=None):
    h = len(calendar) - cfg.holdout_days
    lines = ["# Codex 個股訊號進出研究", "",
             f"資料：{calendar[0]}～{calendar[-1]}；股票池上限 {args.universe}；調整來源 {args.adjust_source}。",
             f"走勢外推：{calendar[cfg.min_train_days]}～{calendar[h - 1]}；最終保留：{calendar[h]}～{calendar[-1]}（126 個交易日）。", "",
             "## 選規則的方法", "",
             f"每檔、每版分開選一個買訊與一個賣訊。每 {cfg.test_days} 個交易日重選，最多使用前 {cfg.train_days} 日；"
             f"第一次需 {cfg.min_train_days} 日。訓練窗前 2/3 為探索，後 1/3 為確認。"
             f"先以固定 MA20 跌破賣訊篩買訊，保留 {cfg.shortlist_buys} 個，再與全部可用賣訊配對；"
             f"探索分數為正的前 {cfg.finalists} 組才能進確認窗。", "",
             f"探索至少 {cfg.min_discovery} 筆、確認至少 {cfg.min_confirmation} 筆已完成且互不重疊的交易。"
             f"分數＝n/(n+{cfg.shrink:g})×平均淨超額－z×max(超額標準差,2%)/√n；探索 z=1，"
             f"確認 z=√(2 log({cfg.finalists}/0.10))。確認最高分仍須大於零，否則持有現金。"
             "訓練期末未平倉以最後可知開盤價估值並計雙邊成本，列入分數但不充當已完成筆數。", "",
             "分段搜尋避免用整個訓練窗暴力挑最佳配對；收縮、小樣本門檻、確認懲罰與現金選項抑制過度配適。"
             "買訊預篩可能錯失只適合其他賣訊的組合，這是為可解釋性與速度接受的取捨。"
             "懲罰不是正式多重檢定或跨股票 FDR 保證。兩版與門檻均事先固定，不依保留期結果再調參。", "",
             "## 成交、成本與市場基準", "",
             "收盤 t 發訊，僅在下一個交易所交易日開盤買進；無有效成交、缺列，或開盤 ≥ 前收×1.095 均略過，不延後追買。"
             "持倉當日收盤即可觸發賣訊；決策在下一個有效開盤執行，停牌則延後。"
             "同股每版最多一筆持倉，賣出當日收盤不再進場。股票池資格只在買訊日檢查，離開股票池不強制賣出。", "",
             "無固定持有期。第 250 個持倉交易所交易日收盤觸發上限，正常於買進後第 250 日開盤賣出；"
             "停牌可使實際成交超過上限。停損版於收盤 ≤ 進場開盤×92%，或 ≤ 進場以來最高收盤（含進場開盤初值）×88% 時決策賣出。"
             "隔日跳空按實際開盤成交，不假設成交在停損線。無額外停損版仍可選到訊號庫中的 ATR 類賣訊。", "",
             f"Costs：手續費單邊 0.1425%×{args.fee_discount:g}、賣出證交稅 0.3%、單邊滑價 {args.slippage:.4%}。"
             "淨報酬＝(賣價/買價)×(1−賣出成本)/(1＋買入成本)−1。", "",
             "市場為當時股票池每日等權、每日再平衡的還原價開盤至開盤指數；開盤 p 採前一日收盤已知的成分。"
             "交易超額＝個股淨報酬減同進出開盤期間市場複利報酬。基準不扣成本，作為較嚴格的門檻。"
             "缺有效開盤沿用上次成交價，恢復交易才反映跳空；尚無價格的成分報酬記零，無法建模下市回收損失。", "",
             "## 樣本外結果（已平倉）", "",
             "跨外推折的持倉保留原賣訊，不因重選規則平倉。外推與最終保留期各自從現金開始；"
             "保留期規則只用其開始前資料選一次並凍結，外推持倉不帶入保留期。"
             "各筆依買訊所屬樣本及折次歸類，持有日為交易所交易日差。", "",
             "| 版本 | 樣本 | 交易數 | 每筆平均超額 | 勝市場率 | cluster t（群數） | 平均淨報酬 | 平均持有日 | 每100持有日超額 | 最差淨報酬 | 上限出場占比 | 最差超額 | 超過250日筆數 |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for variant in VARIANTS:
        for sample, name in (("walk_forward", "走勢外推"), ("holdout", "最終保留")):
            rows = trades[(trades.variant == variant) & (trades["sample"] == sample)]
            lines.append("| " + " | ".join([LABEL[variant], name] + metrics(rows)) + " |")
    lines += ["", f"cluster t 使用 tuning.evaluate.t_cluster，依買訊日每 {CLUSTER_DAYS} 個交易所交易日分群；"
              "群數少時不宜推論，長持倉跨群仍可能相關。每100日超額＝100×超額總和/持有日總和。"
              "這是每筆交易研究，沒有資金配置、槓桿、現金權重的完整投資組合回測；平均交易超額為正不等於投資組合勝市場。", "",
              "## 期末未平倉與截尾檢查", "",
              "期末未成交部位不假裝平倉。下表把已平倉加上未平倉最後可知開盤估值（含預估賣出成本），"
              "檢查只看已平倉的偏差；估值不是可實現成交，停牌或下市價格可能過時。", "",
              "| 版本 | 樣本 | 未平倉數 | 未平倉平均估值超額 | 含估值平均超額 | 含估值平均淨報酬 |",
              "|---|---|---:|---:|---:|---:|"]
    for variant in VARIANTS:
        for sample, name in (("walk_forward", "走勢外推"), ("holdout", "最終保留")):
            a = trades[(trades.variant == variant) & (trades["sample"] == sample)]
            b = terminal[(terminal.variant == variant) & (terminal["sample"] == sample)]
            both = pd.concat([a, b], ignore_index=True)
            fmt = lambda x: "—" if not np.isfinite(x) else f"{x:.3%}"
            lines.append(f"| {LABEL[variant]} | {name} | {len(b)} | {fmt(b.excess.mean())} | "
                         f"{fmt(both.excess.mean())} | {fmt(both.net.mean())} |")
    lines += ["", "## 最常選用的規則", "",
              "次數為股票×重選折數；分開列版本與樣本，現金表示未通過證據門檻。", ""]
    for variant in VARIANTS:
        for sample, name in (("walk_forward", "走勢外推"), ("holdout", "最終保留")):
            sub = [r for r in selected if r[1] == variant and r[2] == sample]
            counts = Counter((r[-2], r[-1]) for r in sub)
            cash = counts.pop(("現金", "現金"), 0)
            lines += [f"### {LABEL[variant]}／{name}", "",
                      f"股票×折數 {len(sub)}；現金 {cash}（{cash / max(len(sub), 1):.1%}）。", "",
                      "| 買訊 | 賣訊 | 次數 |", "|---|---|---:|"]
            for (b, s), count in counts.most_common(10):
                lines.append(f"| {b}：{ind.BUY_DESC.get(b, b)} | {s}：{ind.SELL_DESC.get(s, s)} | {count} |")
            if not counts:
                lines.append("| 無規則通過 | — | 0 |")
            lines.append("")
    lines += ["## 資料與執行限制", "",
              f"股票數 {diagnostics['stocks']}；股票池成分區間 {diagnostics['member_intervals']:,}，"
              f"其中缺有效開盤區間 {diagnostics['stale']:,}。工作程序 {args.workers}；總耗時 {elapsed:.1f} 秒。",
              "prepare 的歷史資料品質、還原事件覆蓋與輔助資料時間戳仍會影響結果；未覆蓋除權息可能扭曲報酬。"]
    if coverage is not None and len(coverage):
        lines += ["", "還原資料摘要（prepare 原始欄位，各列最多10項）：", ""]
        for col in coverage.columns:
            counts = coverage[col].astype(str).value_counts().head(10)
            lines.append(f"- {col}：" + "；".join(f"{str(k).replace('|', '/')}={v}" for k, v in counts.items()))
    lines += ["", "實作未讀取或採用其他研究程式的選規則設計。此報告不宣稱策略已勝市場；"
              "請同時比較兩版的外推、保留期、未平倉估值與交易數。保留期一旦檢視，就不能再當成新的未見樣本。", ""]
    return "\n".join(lines)


def main(argv=None):
    args = parse_args(argv)
    cfg = Config(train_days=args.train_days, min_train_days=args.min_train_days,
                 test_days=args.test_days, rebalance_days=args.rebalance_days)
    costs = Costs(discount=args.fee_discount, slippage=args.slippage)
    t0 = time.monotonic()
    prep = prepare(args, args.universe, args.years, end_date=args.end_date, cfg=cfg)
    calendar = [str(d) for d in prep["calendar"]]
    make_folds(len(calendar), cfg)
    members = membership_arrays(prep, calendar)
    bench, diag = benchmark_index(prep["adjusted"], calendar, members)
    diag["stocks"] = len(prep["adjusted"])
    tasks = ((str(sid), g, calendar, members[str(sid)], bench, costs, cfg)
             for sid, g in prep["adjusted"].items())
    trades, choices, terminal = [], [], []
    # Compute signals/paths once per stock. Return only compact OOS payloads.
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for i, (tr, ch, te) in enumerate(pool.map(_stock_job, tasks, chunksize=1), 1):
            trades.extend(tr)
            choices.extend(ch)
            terminal.extend(te)
            if i % 25 == 0 or i == diag["stocks"]:
                log(f"[research_codex] {i}/{diag['stocks']} stocks, {len(trades)} closed OOS trades; "
                    f"elapsed {time.monotonic() - t0:.0f}s")
    tr = pd.DataFrame(trades, columns=TRADE_COLUMNS)
    te = pd.DataFrame(terminal, columns=TRADE_COLUMNS)
    report = build_report(tr, choices, te, calendar, cfg, args, diag,
                          time.monotonic() - t0, prep.get("coverage"))
    output = Path(args.out_dir) / "research_codex.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    log(f"[research_codex] report: {output}")


if __name__ == "__main__":
    main()
