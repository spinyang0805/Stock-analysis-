// 帳戶檢視切換：合併全部 / 分帳戶列出 / 只看某帳戶（記在 localStorage）
import React, { useEffect, useState } from "react";
import { Landmark } from "lucide-react";

export const ALL = "__all";
export const SPLIT = "__split";

export function useAccountView(accounts, key = "account.view") {
  const [view, setView] = useState(() => { try { return localStorage.getItem(key) || ALL; } catch { return ALL; } });
  useEffect(() => { try { localStorage.setItem(key, view); } catch { /* ignore */ } }, [key, view]);
  // 選的帳戶已不存在 → 回到合併
  const valid = view === ALL || view === SPLIT || accounts.includes(view);
  return [valid ? view : ALL, setView];
}

export default function AccountPicker({ accounts, value, onChange, allowSplit = true }) {
  if (accounts.length <= 1) return null;
  return (
    <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
      <Landmark size={16} aria-hidden="true" className="muted" />
      <select className="input" style={{ minHeight: 36, padding: "4px 10px", fontSize: 14, width: "auto" }} value={value} onChange={(e) => onChange(e.target.value)} aria-label="帳戶檢視">
        <option value={ALL}>全部帳戶（合併）</option>
        {allowSplit && <option value={SPLIT}>全部帳戶（分開列出）</option>}
        {accounts.map((a) => <option key={a} value={a}>只看 {a}</option>)}
      </select>
    </label>
  );
}
