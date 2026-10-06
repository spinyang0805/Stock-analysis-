import React, { useEffect, useId, useRef } from "react";
import { X } from "lucide-react";
import { trendColor } from "../lib/format.js";

export function Card({ title, icon: Icon, right, children, className = "", style }) {
  return (
    <section className={`card ${className}`} style={style}>
      {title && (
        <h3 className="card-title">
          {Icon && <Icon size={16} aria-hidden="true" />}
          {title}
          {right && <span className="right">{right}</span>}
        </h3>
      )}
      {children}
    </section>
  );
}

export function Row({ label, value, color, className = "" }) {
  return (
    <div className="row">
      <span>{label}</span>
      <b className={`num ${className}`} style={color ? { color } : undefined}>{value}</b>
    </div>
  );
}

export function Conclusion({ text, color }) {
  if (!text) return null;
  return <div className="conclusion" style={{ borderLeftColor: color || "var(--flat)" }}>{text}</div>;
}

export function Badge({ children, tone = "" }) {
  return <span className={`badge ${tone}`}>{children}</span>;
}

export function toneOf(v) {
  const n = Number(v);
  return !Number.isFinite(n) || n === 0 ? "flat" : n > 0 ? "up" : "down";
}

export function Seg({ options, value, onChange, label }) {
  return (
    <div className="seg" role="group" aria-label={label}>
      {options.map((o) => {
        const v = typeof o === "string" ? o : o.value;
        const l = typeof o === "string" ? o : o.label;
        return (
          <button key={v} type="button" aria-pressed={value === v} onClick={() => onChange(v)}>{l}</button>
        );
      })}
    </div>
  );
}

export function Tabs({ tabs, value, onChange }) {
  const ref = useRef(null);
  function onKey(e) {
    const idx = tabs.findIndex((t) => t.id === value);
    let next = null;
    if (e.key === "ArrowRight") next = tabs[(idx + 1) % tabs.length];
    if (e.key === "ArrowLeft") next = tabs[(idx - 1 + tabs.length) % tabs.length];
    if (next) {
      e.preventDefault();
      onChange(next.id);
      ref.current?.querySelector(`[data-id="${next.id}"]`)?.focus();
    }
  }
  return (
    <div className="tabs" role="tablist" ref={ref} onKeyDown={onKey}>
      {tabs.map((t) => (
        <button key={t.id} data-id={t.id} type="button" role="tab" className="tab"
          aria-selected={value === t.id} tabIndex={value === t.id ? 0 : -1} onClick={() => onChange(t.id)}>
          {t.icon && <t.icon size={15} aria-hidden="true" />}
          {t.label}
          {t.star && <span className="star" aria-hidden="true">★</span>}
        </button>
      ))}
    </div>
  );
}

export function Kpi({ label, value, sub, subValue, tone }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className={`value num ${tone || ""}`}>{value}</div>
      {(sub || subValue != null) && <div className={`sub num ${tone || ""}`}>{sub}</div>}
    </div>
  );
}

export function Modal({ title, onClose, children, actions }) {
  const id = useId();
  const ref = useRef(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const prev = document.activeElement;
    ref.current?.querySelector("input,select,textarea,button")?.focus();
    const onKey = (e) => { if (e.key === "Escape") closeRef.current(); };
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("keydown", onKey); prev?.focus?.(); };
  }, []);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby={id} ref={ref}>
        <div style={{ display: "flex", alignItems: "center" }}>
          <h2 id={id} style={{ flex: 1 }}>{title}</h2>
          <button type="button" className="btn ghost icon" onClick={onClose} aria-label="關閉"><X size={18} /></button>
        </div>
        {children}
        {actions && <div className="actions">{actions}</div>}
      </div>
    </div>
  );
}

export function Meter({ value, color }) {
  return <div className="meter" role="presentation"><i style={{ width: `${Math.max(2, Math.min(100, value))}%`, background: color }} /></div>;
}

export function Empty({ icon: Icon, title, children }) {
  return (
    <div className="empty">
      {Icon && <Icon size={36} aria-hidden="true" />}
      <div style={{ fontWeight: 700, color: "var(--fg-2)" }}>{title}</div>
      {children}
    </div>
  );
}

export function Signed({ value, digits = 2, suffix = "" }) {
  const n = Number(value);
  if (!Number.isFinite(n)) return <span className="num flat">--</span>;
  return (
    <span className="num" style={{ color: trendColor(n) }}>
      {n > 0 ? "▲+" : n < 0 ? "▼−" : ""}{Math.abs(n).toLocaleString("zh-TW", { maximumFractionDigits: digits })}{suffix}
    </span>
  );
}
