// 盤中即時報價輪詢：開盤時段、分頁可見時每 5 秒；收盤後抓一次（拿當日收盤）
import { useEffect, useRef, useState } from "react";
import { fetchQuotes } from "./market.js";
import { isTradingSession } from "./format.js";
import { supabaseConfigured } from "./supabase.js";

export const QUOTE_POLL_MS = 5000;

export function useQuotes(codes, { paused = false } = {}) {
  const key = [...new Set((codes || []).filter(Boolean))].sort().join(",");
  const [state, setState] = useState({ quotes: {}, source: null, updatedAt: null, error: "", live: isTradingSession() });
  const timer = useRef(null);

  useEffect(() => {
    if (!key || !supabaseConfigured) return undefined;
    let alive = true;
    const list = key.split(",");
    let first = true;
    let running = false;
    async function tick() {
      if (running) return;
      running = true;
      clearTimeout(timer.current);
      const live = isTradingSession();
      if (!document.hidden && !paused && (live || first)) {
        first = false;
        try {
          const r = await fetchQuotes(list);
          if (!alive) return;
          setState((s) => ({ quotes: { ...s.quotes, ...r.quotes }, source: r.source, updatedAt: new Date(), error: "", live }));
        } catch (e) {
          if (alive) setState((s) => ({ ...s, error: e.message, live }));
        }
      }
      running = false;
      // 收盤時段每分鐘檢查一次是否開盤，不打 API
      if (alive) timer.current = setTimeout(tick, live ? QUOTE_POLL_MS : 60000);
    }
    tick();
    const onVis = () => { if (!document.hidden && isTradingSession()) tick(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { alive = false; clearTimeout(timer.current); document.removeEventListener("visibilitychange", onVis); };
  }, [key, paused]);

  return state;
}

// 報價超過 30 秒沒更新 → 視為延遲
export function isStale(updatedAt, live) {
  return live && updatedAt && Date.now() - updatedAt.getTime() > 30000;
}
