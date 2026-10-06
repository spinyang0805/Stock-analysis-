// App 殼層：導覽（桌機側欄／手機底部列）+ hash 路由
// 個股頁、持股、交易、AI、批次工具、帳號管理各自在 src/pages/
import React, { lazy, Suspense, useEffect, useState } from "react";
import {
  Bot, ChevronsLeft, ChevronsRight, LineChart, ListOrdered, LogIn, LogOut, Users, Wallet, Wrench, UserCircle2,
} from "lucide-react";
import { AppStateProvider, useApp } from "./lib/appState.jsx";
import { navigate, useRoute } from "./lib/router.js";
import { Modal } from "./components/ui.jsx";
import ErrorBoundary from "./components/ErrorBoundary.jsx";
import StockPage from "./pages/StockPage.jsx";
import PortfolioPage from "./pages/PortfolioPage.jsx";
import TransactionsPage from "./pages/TransactionsPage.jsx";

const BatchPage = lazy(() => import("./BatchPage.jsx"));
const LegacyAI = lazy(() => import("./pages/LegacyPages.jsx").then((m) => ({ default: m.AIChatPage })));
const LegacyAdmin = lazy(() => import("./pages/LegacyPages.jsx").then((m) => ({ default: m.AdminPage })));

function lastStock() {
  try { return localStorage.getItem("lastStock") || "2330"; } catch { return "2330"; }
}

const NAV = [
  { id: "stock", label: "個股", icon: LineChart, href: () => `/stock/${lastStock()}` },
  { id: "portfolio", label: "我的持股", short: "持股", icon: Wallet, href: () => "/portfolio" },
  { id: "transactions", label: "交易紀錄", short: "交易", icon: ListOrdered, href: () => "/transactions" },
  { id: "ai", label: "AI 選股", short: "AI", icon: Bot, href: () => "/ai" },
  { id: "batch", label: "資料維護", icon: Wrench, href: () => "/batch", role: "admin" },
  { id: "admin", label: "帳號管理", icon: Users, href: () => "/admin", role: "admin" },
];

function Shell() {
  const app = useApp();
  const route = useRoute();
  const [collapsed, setCollapsed] = useState(() => { try { return localStorage.getItem("navCollapsed") === "1"; } catch { return false; } });
  const [account, setAccount] = useState(false);
  const page = route.parts[0] || "stock";

  useEffect(() => { try { localStorage.setItem("navCollapsed", collapsed ? "1" : "0"); } catch { /* ignore */ } }, [collapsed]);
  useEffect(() => { if (!route.parts.length) navigate(`/stock/${lastStock()}`, {}, { replace: true }); }, [route.parts.length]);
  useEffect(() => { window.scrollTo(0, 0); }, [page]);

  const items = NAV.filter((n) => !n.role || app.role === n.role);
  const go = (n) => (e) => { e.preventDefault(); navigate(n.href()); };
  const name = app.user?.user_metadata?.full_name || app.user?.email?.split("@")[0];

  return (
    <div className="shell">
      <nav className={`sidenav ${collapsed ? "collapsed" : ""}`} aria-label="主選單">
        <div className="brand">
          <LineChart size={22} aria-hidden="true" />
          <div className="brand-text">台股看盤<small>TW STOCK DECISION</small></div>
        </div>
        {items.map((n) => (
          <a key={n.id} href={`#${n.href()}`} className="nav-item" aria-current={page === n.id ? "page" : undefined} onClick={go(n)} title={collapsed ? n.label : undefined}>
            <n.icon size={20} aria-hidden="true" /><span className="nav-label">{n.label}</span>
          </a>
        ))}
        <div className="nav-foot">
          {app.user ? (
            <>
              <div className="user-chip">
                {app.user.user_metadata?.avatar_url ? <img src={app.user.user_metadata.avatar_url} alt="" /> : <span className="avatar">{(app.user.email || "?")[0].toUpperCase()}</span>}
                {!collapsed && <div style={{ minWidth: 0 }}><div className="name">{name}</div><div className="dim">{app.role === "admin" ? "管理員" : app.role === "vip" ? "VIP" : "會員"}</div></div>}
              </div>
              <button type="button" className="btn sm" onClick={app.signOut} aria-label="登出"><LogOut size={14} />{!collapsed && "登出"}</button>
            </>
          ) : (
            <button type="button" className="btn primary sm" onClick={app.signIn} disabled={!app.configured}><LogIn size={14} />{!collapsed && "Google 登入"}</button>
          )}
          <button type="button" className="btn ghost sm" onClick={() => setCollapsed((c) => !c)} aria-label={collapsed ? "展開選單" : "收合選單"}>
            {collapsed ? <ChevronsRight size={16} /> : <ChevronsLeft size={16} />}
          </button>
        </div>
      </nav>

      <main className="main">
        {!app.configured && (
          <div className="page" style={{ paddingBottom: 0 }}>
            <div className="notice warn">尚未設定 Supabase 金鑰（VITE_SUPABASE_ANON_KEY），即時報價、持股與登入功能暫停；行情與分析仍可使用。</div>
          </div>
        )}
        <ErrorBoundary resetKey={route.path}>
        <Suspense fallback={<div className="page dim">載入中…</div>}>
          {page === "stock" && <StockPage />}
          {page === "portfolio" && <PortfolioPage />}
          {page === "transactions" && <TransactionsPage />}
          {page === "ai" && <LegacyAI />}
          {page === "batch" && app.role === "admin" && <BatchPage />}
          {page === "admin" && app.role === "admin" && <div className="page"><LegacyAdmin /></div>}
        </Suspense>
        </ErrorBoundary>
      </main>

      <nav className="bottomnav" aria-label="主選單">
        {NAV.slice(0, 4).map((n) => (
          <a key={n.id} href={`#${n.href()}`} className="nav-item" aria-current={page === n.id ? "page" : undefined} onClick={go(n)}>
            <n.icon size={20} aria-hidden="true" /><span>{n.short || n.label}</span>
          </a>
        ))}
        <button type="button" className="nav-item" onClick={() => (app.user ? setAccount(true) : app.signIn())}>
          {app.user ? <UserCircle2 size={20} aria-hidden="true" /> : <LogIn size={20} aria-hidden="true" />}
          <span>{app.user ? "帳號" : "登入"}</span>
        </button>
      </nav>

      {account && app.user && (
        <Modal title="帳號" onClose={() => setAccount(false)}>
          <div className="user-chip" style={{ marginBottom: 12 }}>
            {app.user.user_metadata?.avatar_url ? <img src={app.user.user_metadata.avatar_url} alt="" /> : <span className="avatar">{(app.user.email || "?")[0].toUpperCase()}</span>}
            <div><div className="name">{name}</div><div className="dim">{app.user.email}</div></div>
          </div>
          <div className="grid" style={{ gap: 8 }}>
            {items.slice(4).map((n) => (
              <button key={n.id} type="button" className="btn" onClick={() => { setAccount(false); navigate(n.href()); }}><n.icon size={16} />{n.label}</button>
            ))}
            <button type="button" className="btn danger" onClick={() => { setAccount(false); app.signOut(); }}><LogOut size={16} />登出</button>
          </div>
        </Modal>
      )}
    </div>
  );
}

export default function App() {
  return (
    <AppStateProvider>
      <Shell />
    </AppStateProvider>
  );
}
