// 組合拳「我有買」：使用者自己的實際買賣價（Supabase combo_marks），跟紙上追蹤分開比對
// supabase 延後載入，讓純函式可以在 node 測試裡直接 import
const db = () => import("./supabase.js");

export const markKey = (p) => `${p.stock_id}|${p.signal_date}`;
export const MY_STOP = 0.10; // 跟紙上規則一樣：從自己的買價跌 10% 停損

// 還沒買的持有中部位：盤中價相對紙上買價的燈號（事前登記：.omc/plans/debate/preregistration_late_entry.md）
// 回測（history/out/research_late_entry.md）：晚買 +3%～+8%、甚至漲超過 8% 都不比 −5%～+3% 差 → 不分「漲多不追」；
// 只有跌超過 5% 明顯較差。跌超過 5% 但結構沒壞的那組 t 只有 1.8，沒過事前標準，所以仍是紅燈。
export function lateBuyLight(price, entry) {
  if (!(price > 0) || !(entry > 0)) return null;
  const r = Math.round((price / entry - 1) * 1e6) / 1e6; // 去掉浮點誤差，剛好 −5% 算綠燈
  if (r < -0.05) return { key: "red_lo", color: "red", label: "跌超過 5%，先不買", r };
  return { key: "ok", color: "green", label: "還可以買", r };
}

// 我的報酬：賣了用賣價，沒賣用即時價（沒有就用最近收盤）；未扣手續費與稅
export function myReturn(mark, price) {
  const px = mark.sell_price ?? price;
  return px > 0 && mark.buy_price > 0 ? px / mark.buy_price - 1 : null;
}

// 有標記的部位：我的平均報酬 vs 同一批紙上平均報酬
export function compareMine(marks, rows) {
  const out = [];
  for (const r of rows) {
    const m = marks[markKey(r.p)];
    if (!m) continue;
    const mine = myReturn(m, r.price);
    if (mine == null || r.paper == null) continue;
    out.push({ mine, paper: r.paper, gap: r.p.entry_price > 0 ? m.buy_price / r.p.entry_price - 1 : null });
  }
  if (!out.length) return null;
  const avg = (k) => { const v = out.map((x) => x[k]).filter((x) => x != null); return v.length ? v.reduce((s, x) => s + x, 0) / v.length : null; };
  return { n: out.length, mine: avg("mine"), paper: avg("paper"), gap: avg("gap") };
}

export async function loadMarks() {
  const { supabase, supabaseConfigured } = await db();
  if (!supabaseConfigured) return {};
  const { data, error } = await supabase.from("combo_marks").select("*");
  if (error) throw error;
  return Object.fromEntries((data || []).map((m) => [markKey(m), { ...m, buy_price: Number(m.buy_price), sell_price: m.sell_price == null ? null : Number(m.sell_price) }]));
}

export async function saveMark(m) {
  const { supabase } = await db();
  const row = { stock_id: m.stock_id, signal_date: m.signal_date, buy_date: m.buy_date, buy_price: m.buy_price,
    sell_date: m.sell_date || null, sell_price: m.sell_price || null, updated_at: new Date().toISOString() };
  const { error } = await supabase.from("combo_marks").upsert(row, { onConflict: "user_id,stock_id,signal_date" });
  if (error) throw error;
}

export async function removeMark(m) {
  const { supabase } = await db();
  const { error } = await supabase.from("combo_marks").delete().eq("stock_id", m.stock_id).eq("signal_date", m.signal_date);
  if (error) throw error;
}
