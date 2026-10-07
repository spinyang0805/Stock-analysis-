// AI 選股聊天（Supabase RPC ai_pick：盤後選股清單 + Groq）、帳號管理
import React, { useEffect, useRef, useState } from "react";
import { supabase } from "../lib/supabase.js";
import { useApp } from "../lib/appState.jsx";

export function AIChatPage() {
  const [messages, setMessages] = useState([{
    role:"assistant",
    content:"您好！我是 AI 選股助理。\n請描述您想找的股票條件，例如：\n• 找近期突破月線的強勢股\n• 推薦技術面黃金交叉的股票\n• 哪些股票量增價漲且籌碼集中？"
  }]);
  const app = useApp();
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  const quickActions = [
    "找近期突破月線的強勢股",
    "推薦技術面黃金交叉的股票",
    "哪些股票量增價漲？",
    "找 RSI 超賣可能反彈的股票",
  ];

  useEffect(()=>{ bottomRef.current?.scrollIntoView({behavior:"smooth"}); },[messages, loading]);


  async function send(text) {
    const content = (text||input).trim();
    if (!content||loading) return;
    setInput("");
    const newMsgs = [...messages, {role:"user", content}];
    setMessages(newMsgs);
    setLoading(true);
    try {
      const { data: json, error } = await supabase.rpc("ai_pick", { messages: newMsgs.slice(1).filter(m=>m.role!=="system") });
      if (error) throw new Error(error.message);
      const reply = json?.text ?? (json?.error ? `AI 服務錯誤：${json.error}` : "");
      setMessages(prev=>[...prev,{role:"assistant", content:reply||"AI 未回傳內容，請再試一次"}]);
    } catch(e) {
      setMessages(prev=>[...prev,{role:"assistant", content:`連線失敗：${e.message}`}]);
    }
    setLoading(false);
  }

  if (!app.user) {
    return (
      <div className="page"><div className="card" style={{ textAlign:"center", padding:40 }}>
        <div style={{ fontWeight:700, marginBottom:12 }}>AI 選股需要登入（每人每小時 30 次）</div>
        <button type="button" className="btn primary" onClick={app.signIn}>Google 登入</button>
      </div></div>
    );
  }

  return (
    <div style={{ display:"flex", flexDirection:"column", height:"calc(100dvh - var(--chat-offset, 0px))", minHeight:480, background:"#020617", color:"#f1f5f9" }}>
      <div style={{ padding:"14px 20px", borderBottom:"1px solid #1e293b", background:"#0f172a", flexShrink:0 }}>
        <div style={{ color:"#38bdf8", fontSize:11, fontWeight:800, letterSpacing:1 }}>TW STOCK DECISION SYSTEM</div>
        <div style={{ fontSize:18, fontWeight:900, marginTop:4 }}>AI 選股助理<span style={{ fontSize:12, color:"#94a3b8", fontWeight:500, marginLeft:8 }}>（依盤後選股清單推薦・每小時 30 次）</span></div>
      </div>
      <div style={{ padding:"8px 14px", display:"flex", gap:8, flexWrap:"wrap", borderBottom:"1px solid #1e293b", flexShrink:0 }}>
        {quickActions.map(q=>(
          <button key={q} onClick={()=>send(q)} disabled={loading}
            style={{ padding:"5px 12px", borderRadius:20, border:"1px solid #334155", background:"#1e293b",
              color:"#94a3b8", cursor:"pointer", fontSize:12, whiteSpace:"nowrap" }}>{q}</button>
        ))}
      </div>
      <div style={{ flex:1, overflow:"auto", padding:"12px 16px" }}>
        {messages.map((m,i)=>(
          <div key={i} style={{ margin:"10px 0", display:"flex", justifyContent:m.role==="user"?"flex-end":"flex-start" }}>
            <div style={{ maxWidth:"82%", padding:"10px 14px", borderRadius:12,
              background:m.role==="user"?"#1d4ed8":"#1e293b", color:"#f1f5f9", fontSize:13,
              lineHeight:1.8, whiteSpace:"pre-wrap" }}>
              {m.role==="assistant"&&<div style={{ color:"#38bdf8", fontSize:11, marginBottom:4, fontWeight:700 }}>AI 助理</div>}
              {m.content}
            </div>
          </div>
        ))}
        {loading&&(
          <div style={{ display:"flex", justifyContent:"flex-start", margin:"10px 0" }}>
            <div style={{ padding:"10px 14px", borderRadius:12, background:"#1e293b", color:"#64748b", fontSize:13 }}>
              AI 分析中，正在查詢資料庫...
            </div>
          </div>
        )}
        <div ref={bottomRef}/>
      </div>
      <div style={{ padding:"12px 16px", borderTop:"1px solid #1e293b", background:"#0f172a",
        display:"flex", gap:8, flexShrink:0 }}>
        <input value={input} onChange={e=>setInput(e.target.value)}
          onKeyDown={e=>e.key==="Enter"&&!e.shiftKey&&send()} disabled={loading}
          placeholder="問 AI 推薦適合的股票… (Enter 送出)"
          style={{ flex:1, padding:"10px 14px", borderRadius:8, border:"1px solid #334155",
            background:"#020617", color:"white", fontSize:14 }} />
        <button onClick={()=>send()} disabled={loading||!input.trim()}
          style={{ padding:"10px 20px", borderRadius:8, background:loading?"#1e293b":"#2563eb",
            color:"white", border:0, cursor:loading?"default":"pointer", fontWeight:700, fontSize:14 }}>
          送出
        </button>
        {messages.length>2&&(
          <button onClick={()=>setMessages([messages[0]])}
            style={{ padding:"10px 14px", borderRadius:8, border:"1px solid #334155",
              background:"transparent", color:"#475569", cursor:"pointer", fontSize:12 }}>
            清除
          </button>
        )}
      </div>
    </div>
  );
}

export function AdminPage() {
  const [users,   setUsers]   = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    supabase.from("profiles").select("*").order("created_at", { ascending: false })
      .then(({ data }) => { setUsers(data || []); setLoading(false); });
  }, []);

  async function changeRole(id, newRole) {
    await supabase.from("profiles").update({ role: newRole }).eq("id", id);
    setUsers(prev => prev.map(u => u.id === id ? { ...u, role: newRole } : u));
  }

  const roleColor = { admin:"#f97316", vip:"#a78bfa", user:"#94a3b8" };
  return (
    <div style={{ flex:1, overflow:"auto", background:"#020617", color:"#f1f5f9", fontFamily:"Arial,sans-serif", padding:"20px 24px" }}>
      <div style={{ fontSize:22, fontWeight:900, marginBottom:18 }}>帳號管理</div>
      {loading ? <div style={{color:"#475569"}}>載入中...</div> : (
        <table style={{ width:"100%", borderCollapse:"collapse", fontSize:13 }}>
          <thead>
            <tr style={{ borderBottom:"1px solid #1e293b", color:"#64748b", fontSize:11 }}>
              {["Email","名稱","權限","加入時間"].map(h=>(
                <th key={h} style={{ padding:"8px 10px", textAlign:"left", fontWeight:600 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {users.map(u=>(
              <tr key={u.id} style={{ borderBottom:"1px solid rgba(148,163,184,.08)" }}>
                <td style={{ padding:"10px" }}>{u.email}</td>
                <td style={{ padding:"10px" }}>{u.display_name||"--"}</td>
                <td style={{ padding:"10px" }}>
                  <select value={u.role||"user"} onChange={e=>changeRole(u.id, e.target.value)}
                    style={{ padding:"4px 8px", borderRadius:5, border:"1px solid #334155", background:"#1e293b",
                      color:roleColor[u.role]||"#94a3b8", cursor:"pointer", fontSize:12 }}>
                    <option value="user">一般</option>
                    <option value="vip">VIP</option>
                    <option value="admin">管理員</option>
                  </select>
                </td>
                <td style={{ padding:"10px", color:"#475569", fontSize:11 }}>{u.created_at?.slice(0,10)||"--"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
