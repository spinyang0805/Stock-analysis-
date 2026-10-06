// 全站共用狀態：登入者、持股交易、個股設定（存股 / 波段）
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { supabase, supabaseConfigured } from "./supabase.js";
import { computePositions } from "./portfolio.js";

export const AppCtx = createContext(null);
const Ctx = AppCtx;

export function AppStateProvider({ children }) {
  const [user, setUser] = useState(null);
  const [role, setRole] = useState(null);
  const [authReady, setAuthReady] = useState(false);
  const [transactions, setTransactions] = useState([]);
  const [settings, setSettings] = useState({});
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
    if (!user) { setTransactions([]); setSettings({}); return; }
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

  const value = useMemo(() => ({
    configured: supabaseConfigured, authReady, user, role, signIn, signOut,
    transactions, settings, positions, positionOf, styleOf, setStyle, reload, loading, error,
  }), [authReady, user, role, signIn, signOut, transactions, settings, positions, positionOf, styleOf, setStyle, reload, loading, error]);

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useApp() {
  return useContext(Ctx);
}
