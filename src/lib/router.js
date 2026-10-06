// 極簡 hash router：#/stock/2330?tab=entry → { path:"/stock/2330", parts:["stock","2330"], query:{tab:"entry"} }
// 用 hash 才能在 GitHub Pages 子路徑下深連結、重新整理不 404
import { useCallback, useEffect, useState } from "react";

function parse() {
  const raw = window.location.hash.replace(/^#/, "") || "/";
  const [path, qs = ""] = raw.split("?");
  const query = Object.fromEntries(new URLSearchParams(qs));
  return { path, parts: path.split("/").filter(Boolean).map(decodeURIComponent), query };
}

export function buildHash(path, query = {}) {
  const qs = new URLSearchParams(Object.entries(query).filter(([, v]) => v !== undefined && v !== null && v !== "")).toString();
  return `#${path}${qs ? `?${qs}` : ""}`;
}

export function navigate(path, query, { replace = false } = {}) {
  const h = buildHash(path, query);
  if (replace) window.history.replaceState(null, "", h);
  else window.history.pushState(null, "", h);
  window.dispatchEvent(new HashChangeEvent("hashchange"));
}

export function useRoute() {
  const [route, setRoute] = useState(parse);
  useEffect(() => {
    const on = () => setRoute(parse());
    window.addEventListener("hashchange", on);
    window.addEventListener("popstate", on);
    return () => { window.removeEventListener("hashchange", on); window.removeEventListener("popstate", on); };
  }, []);
  const setQuery = useCallback((patch, opts) => {
    const cur = parse();
    navigate(cur.path, { ...cur.query, ...patch }, { replace: true, ...opts });
  }, []);
  return { ...route, setQuery };
}
