// 非追蹤清單股票：提示缺籌碼／基本面，並可送出「加入每日追蹤」（GitHub Actions 每 20 分鐘處理）
import React, { useEffect, useState } from "react";
import { BellPlus } from "lucide-react";
import { supabase, supabaseConfigured } from "../lib/supabase.js";
import { useApp } from "../lib/appState.jsx";

const STATUS = {
  pending: "已排入，約 20～40 分鐘內完成回補（日K 24 個月 + 近 40 日籌碼），完成後重新整理即可看到籌碼面與基本面",
  done: "已加入每日追蹤；網站重新部署後（約數分鐘）就會出現完整資料",
  invalid: "查無此代號，無法加入追蹤",
  error: "回補失敗",
};

export default function TrackBanner({ code }) {
  const app = useApp();
  const [req, setReq] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!supabaseConfigured) return;
    supabase.from("track_requests").select("*").eq("code", code).maybeSingle().then(({ data }) => setReq(data || null));
  }, [code]);

  async function request() {
    setBusy(true); setErr("");
    const { error } = await supabase.from("track_requests").insert({ code });
    setBusy(false);
    if (error && !/duplicate/i.test(error.message)) { setErr(`送出失敗：${error.message}`); return; }
    setReq({ code, status: "pending" });
  }

  return (
    <div className="notice warn" style={{ marginTop: 12, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
      <div style={{ flex: "1 1 260px" }}>
        <b>{code} 不在每日追蹤清單。</b>K 線、技術指標與進出場策略改用 FinMind 即時計算；籌碼面與基本面卡片需加入追蹤後才有資料。
        {req && <div style={{ marginTop: 4 }}>狀態：{STATUS[req.status] || req.status}{req.message && req.status !== "done" ? `（${req.message}）` : ""}</div>}
        {err && <div style={{ marginTop: 4, color: "#fecaca" }}>{err}</div>}
      </div>
      {!req && (app.user ? (
        <button type="button" className="btn primary sm" onClick={request} disabled={busy}><BellPlus size={14} />{busy ? "送出中…" : "加入每日追蹤"}</button>
      ) : (
        <button type="button" className="btn sm" onClick={app.signIn} disabled={!app.configured}>登入後可加入追蹤</button>
      ))}
    </div>
  );
}
