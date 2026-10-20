// 全站共用狀態：登入者、持股交易、個股設定（存股 / 波段）
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { supabase, supabaseConfigured } from "./supabase.js";
import { computePositions } from "./portfolio.js";
import { computeUsPositions, latestUsPrices, valueUsPositions } from "./usPortfolio.js";

export const AppCtx = createContext(null);
const Ctx = AppCtx;

export function AppStateProvider({ children }) {
  const [user, setUser] = useState(null);
  const [role, setRole] = useState(null);
  const [authReady, setAuthReady] = useState(false);
  const [transactions, setTransactions] = useState([]);
  const [settings, setSettings] = useState({});
  const [usTx, setUsTx] = useState([]);
  const [usPriceRows, setUsPriceRows] = useState([]);
  const [usError, setUsError] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!supabaseConfigured) { setAuthReady(true); return; }
    supabase.auth.getSession().then(({ data: { session } }) => { setUser(session?.user ?? null); setAuthReady(true); });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_, session) => setUser(session?.user ?? null));
    return () => subscription.unsubscribe();
  }, []);

  useEffect(() => {
    if (!user) { setRole(null); return; }
    supabase.from("profiles").select("role").eq("id", user.id).single().then(({ data }) => setRole(data?.role ?? "user"));
  }, [user]);

  const reload = useCallback(async () => {
    if (!user) { setTransactions([]); setSettings({}); setUsTx([]); setUsPriceRows([]); setUsError(""); return; }
    setLoading(true); setError("");
    const all = [];
    let txErr = null;
    for (let from = 0; ; from += 1000) {
      const { data, error: e } = await supabase.from("transactions").select("*")
        .order("trade_date", { ascending: true }).order("created_at", { ascending: true }).range(from, from + 999);
      if (e) { txErr = e; break; }
      all.push(...(data || []));
      if (!data || data.length < 1000) break;
    }
    const st = await supabase.from("holding_settings").select("*");
    if (txErr) setError(txErr.message);
    setTransactions(all);
    setSettings(Object.fromEntries((st.data || []).map((s) => [s.stock_id, s])));
    // 美股：資料表還沒建立或查詢失敗時不影響台股，只記錄錯誤
    try {
      const us = [];
      let usErr = null;
      for (let from = 0; ; from += 1000) {
        const { data, error: e } = await supabase.from("us_transactions").select("*").order("trade_date", { ascending: true }).range(from, from + 999);
        if (e) { usErr = e; break; }
        us.push(...(data || []));
        if (!data || data.length < 1000) break;
      }
      const since = new Date(Date.now() - 30 * 864e5).toISOString().slice(0, 10);
      const pr = usErr ? { data: [] } : await supabase.from("us_prices").select("ticker,date,close,source").gte("date", since).order("date", { ascending: false }).limit(5000);
      setUsTx(usErr ? [] : us);
      setUsPriceRows(pr.data || []);
      setUsError(usErr ? usErr.message : pr.error ? pr.error.message : "");
    } catch (e) {
      setUsTx([]); setUsPriceRows([]); setUsError(String(e?.message || e));
    }
    setLoading(false);
  }, [user]);

  useEffect(() => { reload(); }, [reload]);

  const setStyle = useCallback(async (code, style) => {
    setSettings((s) => ({ ...s, [code]: { ...(s[code] || {}), stock_id: code, style } }));
    const { error: e } = await supabase.from("holding_settings").upsert({ stock_id: code, style, updated_at: new Date().toISOString() }, { onConflict: "user_id,stock_id" });
    if (e) setError(e.message);
  }, []);

  const signIn = useCallback(async () => {
    const redirectTo = window.location.origin + window.location.pathname;
    await supabase.auth.signInWithOAuth({ provider: "google", options: { redirectTo } });
  }, []);
  const signOut = useCallback(() => supabase.auth.signOut(), []);

  const positions = useMemo(() => computePositions(transactions), [transactions]);
  const positionOf = useCallback((code) => positions.find((p) => p.code === String(code) && p.shares > 0) || null, [positions]);
  const styleOf = useCallback((code) => settings[code]?.style || "存股", [settings]);

  const usPositions = useMemo(() => computeUsPositions(usTx), [usTx]);
  const usValued = useMemo(() => {
    const { prices, fx } = latestUsPrices(usPriceRows);
    return valueUsPositions(usPositions, prices, fx);
  }, [usPositions, usPriceRows]);

  const value = useMemo(() => ({
    configured: supabaseConfigured, authReady, user, role, signIn, signOut,
    transactions, settings, positions, positionOf, styleOf, setStyle, reload, loading, error,
    usTransactions: usTx, usValued, usError,
  }), [authReady, user, role, signIn, signOut, transactions, settings, usTx, usValued, usError, positions, positionOf, styleOf, setStyle, reload, loading, error]);

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useApp() {
  return useContext(Ctx);
}
