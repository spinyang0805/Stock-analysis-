import math
from typing import Dict, List, Any

import numpy as np
import pandas as pd


def _safe_float(value):
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _signal(level: str, title: str, message: str, score: int, category: str) -> Dict[str, Any]:
    return {
        "level": level,
        "title": title,
        "message": message,
        "score": score,
        "category": category,
    }


def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["MA5"] = df["Close"].rolling(5).mean()
    df["MA10"] = df["Close"].rolling(10).mean()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA60"] = df["Close"].rolling(60).mean()
    df["V_MA5"] = df["Volume"].rolling(5).mean()

    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))

    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    df["BB_MID"] = df["Close"].rolling(20).mean()
    bb_std = df["Close"].rolling(20).std()
    df["BB_UPPER"] = df["BB_MID"] + 2 * bb_std
    df["BB_LOWER"] = df["BB_MID"] - 2 * bb_std
    df["BB_WIDTH"] = (df["BB_UPPER"] - df["BB_LOWER"]) / df["BB_MID"]

    df["CHANGE_PCT"] = df["Close"].pct_change() * 100
    df["PRICE_20D_MAX"] = df["High"].rolling(20).max()
    df["PRICE_20D_MIN"] = df["Low"].rolling(20).min()
    return df


def build_rule_based_analysis(df: pd.DataFrame, stock: str) -> Dict[str, Any]:
    df = enrich_indicators(df).dropna().copy()
    if len(df) < 2:
        return {
            "stock": stock,
            "trend": "資料不足",
            "score": 0,
            "rating": "Neutral",
            "summary": "歷史資料不足，暫時無法完成技術分析。",
            "signals": [],
            "indicators": {},
            "missing_data": ["至少需要 60 日以上日線資料"],
        }

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    signals: List[Dict[str, Any]] = []
    missing_data: List[str] = []

    close = latest["Close"]
    ma5, ma10, ma20, ma60 = latest["MA5"], latest["MA10"], latest["MA20"], latest["MA60"]
    ma60_slope = latest["MA60"] - df.iloc[-6]["MA60"] if len(df) >= 6 else 0

    if ma5 > ma10 > ma20 > ma60 and ma60_slope > 0:
        signals.append(_signal("bullish", "四線多排", "MA5、MA10、MA20、MA60 由上而下排列且 MA60 向上，屬於強勢多頭結構。", 30, "trend"))
    elif ma5 > ma20 > ma60:
        signals.append(_signal("bullish", "多頭排列", "短中長期均線維持多頭排列，趨勢偏多。", 20, "trend"))
    elif ma5 < ma20 < ma60:
        signals.append(_signal("bearish", "空頭排列", "均線呈現空頭排列，價格反彈容易遇到壓力。", -25, "trend"))
    elif abs(ma5 - ma20) / close < 0.015 and abs(ma20 - ma60) / close < 0.035:
        signals.append(_signal("neutral", "均線糾結盤整", "短中長期均線靠近，市場可能仍在方向選擇。", 0, "trend"))

    if latest["Low"] > prev["High"]:
        signals.append(_signal("bullish", "向上跳空缺口", "今日低點高於前一日高點，代表買盤力道明顯。", 16, "price_action"))
    elif latest["High"] < prev["Low"]:
        signals.append(_signal("bearish", "向下跳空缺口", "今日高點低於前一日低點，代表賣壓或利空反應強。", -18, "price_action"))

    if ma5 > ma20 > ma60 and latest["Low"] < ma20 and close > ma20:
        signals.append(_signal("bullish", "回後買上漲", "多頭趨勢中回測月線後收回 MA20，屬於拉回轉強訊號。", 18, "price_action"))

    if latest["BB_WIDTH"] > prev["BB_WIDTH"] * 1.2 and close > latest["BB_UPPER"]:
        signals.append(_signal("bullish", "開布林突破", "布林通道寬度擴張且收盤突破上軌，代表波動放大與攻擊力道增強。", 22, "volatility"))
    elif close < latest["BB_LOWER"]:
        signals.append(_signal("bearish", "跌破布林下軌", "股價跌破布林下軌，短線可能有恐慌賣壓或超跌風險。", -16, "volatility"))

    volume = latest.get("Volume", np.nan)
    vma5 = latest.get("V_MA5", np.nan)
    change = latest.get("CHANGE_PCT", 0)
    if not pd.isna(volume) and not pd.isna(vma5):
        if volume > vma5 and change > 3:
            signals.append(_signal("bullish", "量增價漲", "成交量大於 5 日均量且漲幅大於 3%，屬於健康攻擊訊號。", 18, "volume_price"))
        elif volume < vma5 and change > 0:
            signals.append(_signal("warning", "量縮價漲", "股價上漲但量能低於 5 日均量，追價意願較弱，需觀察續航力。", -4, "volume_price"))
        elif volume > vma5 and change < -3:
            signals.append(_signal("bearish", "量增價跌", "成交量放大但股價重跌，可能是主力出貨或恐慌賣壓。", -22, "volume_price"))
        elif volume < vma5 and change < 0:
            signals.append(_signal("neutral", "量縮價跌", "股價下跌但量能萎縮，可能是多頭回檔或市場冷清。", 2, "volume_price"))
    else:
        missing_data.append("成交量資料不足，量價矩陣略過")

    rsi = latest["RSI14"]
    if rsi >= 75:
        signals.append(_signal("warning", "RSI 過熱", "RSI 高於 75，短線追價風險提高。", -8, "momentum"))
    elif rsi <= 30:
        signals.append(_signal("warning", "RSI 超賣", "RSI 低於 30，短線可能反彈，但需搭配止跌訊號。", 6, "momentum"))

    if latest["MACD"] > latest["MACD_SIGNAL"] and prev["MACD"] <= prev["MACD_SIGNAL"]:
        signals.append(_signal("bullish", "MACD 黃金交叉", "MACD 由下往上穿越訊號線，動能轉強。", 12, "momentum"))
    elif latest["MACD"] < latest["MACD_SIGNAL"] and prev["MACD"] >= prev["MACD_SIGNAL"]:
        signals.append(_signal("bearish", "MACD 死亡交叉", "MACD 由上往下跌破訊號線，動能轉弱。", -12, "momentum"))

    if close > latest["PRICE_20D_MAX"]:
        signals.append(_signal("bullish", "突破 20 日高點", "股價突破近 20 日高點，若量能同步放大，容易延伸波段。", 14, "breakout"))
    elif close < latest["PRICE_20D_MIN"]:
        signals.append(_signal("bearish", "跌破 20 日低點", "股價跌破近 20 日低點，需留意趨勢轉弱。", -14, "breakout"))

    credit_data_available = False
    chip_data_available = False
    if not credit_data_available:
        missing_data.append("券資比、融資維持率等信用交易資料尚未串接，軋空/斷頭規則暫以技術替代訊號呈現")
    if not chip_data_available:
        missing_data.append("400張大戶、投信買超、散戶持股等籌碼資料尚未串接，籌碼集中度規則暫不評分")

    total_score = sum(s["score"] for s in signals)
    if total_score >= 45:
        trend = "強勢多頭"
        rating = "Strong Bullish"
    elif total_score >= 20:
        trend = "偏多"
        rating = "Bullish"
    elif total_score <= -35:
        trend = "弱勢空頭"
        rating = "Strong Bearish"
    elif total_score <= -15:
        trend = "偏空"
        rating = "Bearish"
    else:
        trend = "盤整觀望"
        rating = "Neutral"

    bullish = [s for s in signals if s["score"] > 0]
    bearish = [s for s in signals if s["score"] < 0]
    summary = f"{stock} 目前綜合分數 {total_score}，判定為「{trend}」。"
    if bullish:
        summary += f" 主要正向訊號：{bullish[0]['title']}。"
    if bearish:
        summary += f" 主要風險訊號：{bearish[0]['title']}。"

    return {
        "stock": stock,
        "trend": trend,
        "score": int(total_score),
        "rating": rating,
        "summary": summary,
        "signals": sorted(signals, key=lambda x: abs(x["score"]), reverse=True),
        "indicators": {
            "close": _safe_float(close),
            "change_pct": _safe_float(change),
            "volume": _safe_float(volume),
            "ma5": _safe_float(ma5),
            "ma10": _safe_float(ma10),
            "ma20": _safe_float(ma20),
            "ma60": _safe_float(ma60),
            "rsi14": _safe_float(rsi),
            "macd": _safe_float(latest["MACD"]),
            "macd_signal": _safe_float(latest["MACD_SIGNAL"]),
            "bb_upper": _safe_float(latest["BB_UPPER"]),
            "bb_mid": _safe_float(latest["BB_MID"]),
            "bb_lower": _safe_float(latest["BB_LOWER"]),
        },
        "missing_data": missing_data,
    }
