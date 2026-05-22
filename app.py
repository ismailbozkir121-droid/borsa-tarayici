import os
import sys

# Streamlit'in kendi python kütüphane yoluna zorla ekleme yapıyoruz
# Bu sayede sistem dışarıdan yükleme engeline takılmayacak
venv_path = "/home/adminuser/venv/lib/python3.14/site-packages"
if venv_path not in sys.path:
    sys.path.append(venv_path)

# Kütüphaneleri en başta sessizce yüklemeyi deniyoruz
try:
    import pip
    pip.main(["install", "--target=" + venv_path, "yfinance", "ta"])
except Exception:
    pass

import yfinance as yf
import ta
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import json
import requests
import urllib.parse
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

st.set_page_config(
    page_title="BIST100 Borsa Tarayıcı",
    page_icon="📈",
    layout="wide",
)

BIST100_TICKERS = [
    "AKBNK.IS", "AKSA.IS", "AKSEN.IS", "ALARK.IS", "ALBRK.IS", "ALKIM.IS",
    "ANACM.IS", "ARCLK.IS", "ASELS.IS", "AYEN.IS", "AYGAZ.IS", "BAGFS.IS",
    "BIMAS.IS", "BRISA.IS", "BRYAT.IS", "BUCIM.IS", "CCOLA.IS", "CIMSA.IS",
    "CLEBI.IS", "DOAS.IS", "DOHOL.IS", "ECILC.IS", "EGEEN.IS", "EKGYO.IS",
    "ENKAI.IS", "EREGL.IS", "EUPWR.IS", "FENER.IS", "FROTO.IS", "GARAN.IS",
    "GESAN.IS", "GLYHO.IS", "GUBRF.IS", "GWIND.IS", "HALKB.IS", "HEKTS.IS",
    "IPEKE.IS", "ISGYO.IS", "ISCTR.IS", "JANTS.IS", "KAREL.IS", "KARTN.IS",
    "KCHOL.IS", "KERVT.IS", "KLNMA.IS", "KONTR.IS", "KOZAA.IS", "KOZAL.IS",
    "KRDMD.IS", "LOGO.IS", "MAVI.IS", "MGROS.IS", "MPARK.IS", "NTHOL.IS",
    "NUHCM.IS", "ODAS.IS", "OTKAR.IS", "OYAKC.IS", "PETKM.IS", "PGSUS.IS",
    "QUAGR.IS", "RGYAS.IS", "SAHOL.IS", "SELEC.IS", "SISE.IS", "SKBNK.IS",
    "SMART.IS", "SOKM.IS", "TATGD.IS", "TAVHL.IS", "TCELL.IS", "THYAO.IS",
    "TKFEN.IS", "TKNSA.IS", "TOASO.IS", "TRGYO.IS", "TTKOM.IS", "TTRAK.IS",
    "TUPRS.IS", "ULKER.IS", "VAKBN.IS", "VESBE.IS", "VESTL.IS", "YKBNK.IS",
    "YYAPI.IS", "ZRGYO.IS", "AEFES.IS", "AGHOL.IS", "BERA.IS", "ENJSA.IS",
    "ESEN.IS", "KARSN.IS", "LMKDC.IS", "METUR.IS", "PARSN.IS", "PENTA.IS",
    "SASA.IS", "SILVR.IS", "TBORG.IS", "TDGYO.IS",
]

TZ_ISTANBUL = pytz.timezone("Europe/Istanbul")

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "settings.json")
_SETTINGS_DEFAULTS = {"phone": "", "apikey": "", "bot_enabled": False}

def load_settings() -> dict:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {**_SETTINGS_DEFAULTS, **data}
    except Exception:
        return _SETTINGS_DEFAULTS.copy()

def save_settings(phone: str, apikey: str, bot_enabled: bool) -> None:
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump({"phone": phone, "apikey": apikey, "bot_enabled": bot_enabled}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

LOG_FILE = os.path.join(os.path.dirname(__file__), "scan_log.csv")
_LOG_COLUMNS = ["Tarih", "Saat", "Kaynak", "Sinyal Sayısı", "Hisseler"]

def append_scan_log(results: list[dict], source: str) -> None:
    now = datetime.now(TZ_ISTANBUL)
    hisseler = ", ".join(r["Hisse"] for r in results[:15]) if results else "-"
    row = {
        "Tarih": now.strftime("%d.%m.%Y"),
        "Saat": now.strftime("%H:%M"),
        "Kaynak": source,
        "Sinyal Sayısı": len(results),
        "Hisseler": hisseler,
    }
    file_exists = os.path.isfile(LOG_FILE)
    try:
        with open(LOG_FILE, "a", encoding="utf-8-sig", newline="") as f:
            import csv as _csv
            writer = _csv.DictWriter(f, fieldnames=_LOG_COLUMNS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception:
        pass

def load_scan_log() -> pd.DataFrame:
    try:
        df = pd.read_csv(LOG_FILE, encoding="utf-8-sig")
        return df[_LOG_COLUMNS].iloc[::-1].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=_LOG_COLUMNS)

_bot_state: dict = {
    "active": False,
    "phone": "",
    "apikey": "",
    "last_run": None,
    "last_count": 0,
    "last_label": "",
    "params": {
        "rsi_min": 30,
        "rsi_max": 45,
        "sma_threshold": 20,
        "atr_multiplier": 1.0,
        "rrr_multiplier": 2.0,
        "min_vol_tl": 50_000_000,
    },
}
_bot_lock = threading.Lock()

@st.cache_data(ttl=3600, show_spinner=False)
def download_data(ticker: str) -> pd.DataFrame:
    try:
        df = yf.download(ticker, period="1y", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return pd.DataFrame()

def download_data_intraday(ticker: str) -> pd.DataFrame:
    try:
        df = yf.download(ticker, period="60d", interval="1h", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return pd.DataFrame()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or len(df) < 60:
        return None
    df = df.copy()
    close = df["Close"].squeeze()
    high = df["High"].squeeze()
    low = df["Low"].squeeze()
    volume = df["Volume"].squeeze()

    df["rsi"] = ta.momentum.RSIIndicator(close=close, window=14).rsi()
    macd_obj = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
    df["macd"] = macd_obj.macd()
    df["macd_signal"] = macd_obj.macd_signal()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["sma50"] = ta.trend.SMAIndicator(close=close, window=50).sma_indicator()
    df["ema50"] = ta.trend.EMAIndicator(close=close, window=50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range()
    df["vol_avg10"]    = volume.rolling(10).mean()
    df["vol_avg20"]    = volume.rolling(20).mean()
    df["vol_tl_avg10"] = (close * volume).rolling(10).mean()
    boll_obj = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
    df["boll_upper"] = boll_obj.bollinger_hband()
    df["boll_lower"] = boll_obj.bollinger_lband()
    return df

@st.cache_data(ttl=1800, show_spinner=False)
def get_market_state() -> dict:
    try:
        df = yf.download("XU100.IS", period="3mo", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if len(df) < 55:
            return {"bullish": None}
        close = df["Close"].squeeze()
        ema50_s = ta.trend.EMAIndicator(close=close, window=50).ema_indicator()
        price   = float(close.iloc[-1])
        ema50_v = float(ema50_s.iloc[-1])
        pct     = (price - ema50_v) / ema50_v * 100
        return {"bullish": price > ema50_v, "price": round(price, 0), "ema50": round(ema50_v, 0), "pct": round(pct, 1)}
    except Exception:
        return {"bullish": None}

def compute_weekly_trend(df: pd.DataFrame) -> dict:
    try:
        weekly = df[["Open", "High", "Low", "Close", "Volume"]].resample("W").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        ).dropna()
        if len(weekly) < 22:
            return {"bullish": False}
        w_close = weekly["Close"].squeeze()
        w_rsi = ta.momentum.RSIIndicator(close=w_close, window=14).rsi().iloc[-1]
        w_ema20 = ta.trend.EMAIndicator(close=w_close, window=20).ema_indicator().iloc[-1]
        w_price = float(w_close.iloc[-1])
        bullish = (not pd.isna(w_rsi)) and (not pd.isna(w_ema20)) and float(w_rsi) > 45 and w_price > float(w_ema20)
        return {"bullish": bullish, "w_rsi": round(float(w_rsi), 1) if not pd.isna(w_rsi) else 0}
    except Exception:
        return {"bullish": False}

BIST100_SECTORS: dict[str, str] = {
    "AKBNK.IS": "Bankacılık", "ALBRK.IS": "Bankacılık", "GARAN.IS": "Bankacılık",
    "HALKB.IS": "Bankacılık", "ISCTR.IS": "Bankacılık", "SKBNK.IS": "Bankacılık",
    "VAKBN.IS": "Bankacılık", "YKBNK.IS": "Bankacılık",
    "EREGL.IS": "Demir-Çelik", "KRDMD.IS": "Demir-Çelik", "OYAKC.IS": "Demir-Çelik",
    "AKSEN.IS": "Enerji",  "AYEN.IS": "Enerji",  "EGEEN.IS": "Enerji",
    "ENJSA.IS": "Enerji",  "EUPWR.IS": "Enerji",  "GWIND.IS": "Enerji", "ODAS.IS": "Enerji",
    "PETKM.IS": "Petrokimya", "SASA.IS": "Petrokimya", "TUPRS.IS": "Petrokimya",
    "DOHOL.IS": "Holding", "GLYHO.IS": "Holding", "KCHOL.IS": "Holding",
    "SAHOL.IS": "Holding", "AGHOL.IS": "Holding", "NTHOL.IS": "Holding",
    "EKGYO.IS": "GYO",  "ISGYO.IS": "GYO",  "RGYAS.IS": "GYO",
    "TRGYO.IS": "GYO",  "YYAPI.IS": "GYO",  "ZRGYO.IS": "GYO", "TDGYO.IS": "GYO",
    "FROTO.IS": "Otomotiv", "TOASO.IS": "Otomotiv", "TTRAK.IS": "Otomotiv",
    "OTKAR.IS": "Otomotiv", "ARCLK.IS": "Dayanıklı Tüketim", "VESBE.IS": "Dayanıklı Tüketim",
    "VESTL.IS": "Dayanıklı Tüketim",
    "BIMAS.IS": "Perakende", "MGROS.IS": "Perakende", "MAVI.IS": "Perakende", "SOKM.IS": "Perakende",
    "ULKER.IS": "G.Sanayi", "TATGD.IS": "G.Sanayi", "CCOLA.IS": "G.Sanayi", "AEFES.IS": "G.Sanayi",
    "TCELL.IS": "Telekom", "TTKOM.IS": "Telekom",
    "THYAO.IS": "Havacılık", "PGSUS.IS": "Havacılık", "CLEBI.IS": "Havacılık",
    "TAVHL.IS": "Havacılık", "MPARK.IS": "Turizm",   "BRYAT.IS": "Turizm",
    "ANACM.IS": "İnşaat Malz.", "BUCIM.IS": "Çimento", "CIMSA.IS": "Çimento",
    "NUHCM.IS": "Çimento",  "ENKAI.IS": "İnşaat",
    "ASELS.IS": "Savunma",  "KAREL.IS": "Teknoloji", "LOGO.IS": "Teknoloji", "SMART.IS": "Teknoloji",
    "GUBRF.IS": "Gübre/Tarım", "BAGFS.IS": "Gübre/Tarım", "KOZAA.IS": "Madencilik",  "KOZAL.IS": "Madencilik",
}

def detect_rsi_divergence(df: pd.DataFrame) -> bool:
    try:
        w = df.tail(20)
        closes = w["Close"].squeeze().values
        rsis   = w["rsi"].values
        if any(pd.isna(rsis)):
            return False
        lows = [i for i in range(1, len(closes) - 1) if closes[i] <= closes[i - 1] and closes[i] <= closes[i + 1]]
        if len(lows) < 2:
            return False
        i1, i2 = lows[-2], lows[-1]
        return float(closes[i2]) < float(closes[i1]) * 0.999 and float(rsis[i2]) > float(rsis[i1])
    except Exception:
        return False

def detect_capitulation_volume(df: pd.DataFrame) -> bool:
    try:
        vol_avg20 = float(df["vol_avg20"].iloc[-1])
        if pd.isna(vol_avg20) or vol_avg20 == 0:
            return False
        recent_vols = df["Volume"].squeeze().iloc[-5:]
        return any(float(v) >= vol_avg20 * 2.0 for v in recent_vols)
    except Exception:
        return False

def detect_candlestick_pattern(df: pd.DataFrame) -> tuple[str, int]:
    if len(df) < 3:
        return ("", 0)
    def _row(i):
        o = float(df["Open"].iloc[i])
        h = float(df["High"].iloc[i])
        l = float(df["Low"].iloc[i])
        c = float(df["Close"].iloc[i])
        body = abs(c - o)
        rng = h - l if h != l else 1e-9
        upper = h - max(o, c)
        lower = min(o, c) - l
        return o, h, l, c, body, rng, upper, lower

    o0, h0, l0, c0, body0, rng0, up0, lo0 = _row(-1)
    o1, h1, l1, c1, body1, rng1, up1, lo1 = _row(-2)
    o2, h2, l2, c2, body2, rng2, up2, lo2 = _row(-3)

    is_doji = body0 < 0.05 * rng0
    is_hammer = body0 < 0.30 * rng0 and lo0 >= 2.0 * body0 and up0 <= 0.5 * max(body0, 0.001 * c0) and not is_doji
    is_shooting_star = body0 < 0.30 * rng0 and up0 >= 2.0 * body0 and lo0 <= 0.5 * max(body0, 0.001 * c0) and not is_doji
    is_bullish_engulfing = c1 < o1 and c0 > o0 and o0 <= c1 and c0 >= o1 and body0 > body1
    is_bearish_engulfing = c1 > o1 and c0 < o0 and o0 >= c1 and c0 <= o1 and body0 > body1
    mid_c2 = (o2 + c2) / 2.0
    is_morning_star = c2 < o2 and body2 > 0.40 * rng2 and body1 < 0.30 * rng1 and c0 > o0 and c0 > mid_c2

    if is_morning_star: return ("Sabah Yıldızı", +30)
    if is_bullish_engulfing: return ("Yutan Boğa", +30)
    if is_hammer: return ("Çekiç Mumu", +30)
    if is_bearish_engulfing: return ("Yutan Ayı", -15)
    if is_shooting_star: return ("Kayan Yıldız", -15)
    if is_doji: return ("Doji", 0)
    return ("", 0)

def scan_tickers_core(tickers: list[str], get_df_fn, rsi_min: float, rsi_max: float, sma_threshold: float, atr_multiplier: float, rrr_multiplier: float, min_vol_tl: float = 50_000_000, progress_cb=None) -> list[dict]:
    results = []
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        if progress_cb: progress_cb(ticker, i + 1, total)
        df = get_df_fn(ticker)
        df = compute_indicators(df)
        if df is None: continue

        last = df.iloc[-1]
        prev = df.iloc[-2]
        rsi_now = last["rsi"]
        rsi_prev = prev["rsi"]
        close_price = float(last["Close"])
        volume_now = float(last["Volume"])
        vol_avg10 = float(last["vol_avg10"]) if not pd.isna(last["vol_avg10"]) else 0
        vol_tl_avg10 = float(last["vol_tl_avg10"]) if not pd.isna(last["vol_tl_avg10"]) else 0
        sma50 = float(last["sma50"]) if not pd.isna(last["sma50"]) else 0
        ema50 = float(last["ema50"]) if not pd.isna(last["ema50"]) else 0
        atr = float(last["atr"]) if not pd.isna(last["atr"]) else 0
        macd_now = last["macd"]
        macd_signal_now = last["macd_signal"]
        macd_hist = float(last["macd_hist"]) if not pd.isna(last["macd_hist"]) else None
        macd_hist_prev = float(prev["macd_hist"]) if not pd.isna(prev["macd_hist"]) else None
        macd_prev = float(prev["macd"]) if not pd.isna(prev["macd"]) else None
        macd_sig_prev = float(prev["macd_signal"]) if not pd.isna(prev["macd_signal"]) else None

        if pd.isna(rsi_now) or atr == 0 or sma50 == 0 or ema50 == 0: continue
        if not (rsi_min <= float(rsi_now) <= rsi_max): continue
        if float(rsi_now) <= float(rsi_prev): continue

        close_s = df["Close"].squeeze()
        vol_s = df["Volume"].squeeze()
        vol_tl_3d = float((close_s * vol_s).iloc[-3:].mean())
        if not pd.isna(vol_tl_3d) and vol_tl_avg10 > 0 and vol_tl_3d < vol_tl_avg10 * 0.8: continue
        if vol_tl_avg10 > 0 and vol_tl_avg10 < min_vol_tl: continue
        if close_price < sma50 * (1 - sma_threshold / 100): continue
        if close_price < ema50: continue

        macd_ok = False
        if macd_hist is not None:
            if macd_hist > 0: macd_ok = True
            elif (macd_hist_prev is not None and macd_hist_prev <= 0 and macd_hist > macd_hist_prev): macd_ok = True
            elif (macd_prev is not None and macd_sig_prev is not None and macd_prev <= macd_sig_prev and float(macd_now) > float(macd_signal_now)): macd_ok = True
        if not macd_ok: continue

        entry = close_price
        stop  = entry - atr_multiplier * atr
        stop_distance = entry - stop
        target = entry + rrr_multiplier * stop_distance
        target_pct = (target - entry) / entry * 100
        stop_pct   = (stop - entry) / entry * 100

        rsi_slope = float(rsi_now) - float(rsi_prev)
        rsi_slope_score = min(40, max(0, rsi_slope * 10))

        if not pd.isna(macd_now) and not pd.isna(macd_signal_now):
            if float(macd_now) > float(macd_signal_now):
                macd_diff = float(macd_now) - float(macd_signal_now)
                macd_score = min(30, max(0, 15 + macd_diff / atr * 10))
            else:
                macd_score = max(0, 10 + (float(macd_now) - float(macd_signal_now)) / atr * 5)
        else: macd_score = 0

        vol_ratio = volume_now / vol_avg10 if vol_avg10 > 0 else 1
        vol_score = min(30, max(0, (vol_ratio - 1.0) * 15))
        base_score = int(round(rsi_slope_score + macd_score + vol_score))

        cdl_label, cdl_adj = detect_candlestick_pattern(df)
        power_score = int(min(100, max(0, base_score + cdl_adj)))

        weekly_info = compute_weekly_trend(df)
        if weekly_info["bullish"]: power_score = int(min(100, power_score + 10))

        rsi_div = detect_rsi_divergence(df)
        if rsi_div: power_score = int(min(100, power_score + 20))

        bb_touch = False
        boll_lowers = df["boll_lower"].iloc[-4:]
        recent_lows = df["Low"].squeeze().iloc[-4:]
        if not boll_lowers.isna().all():
            bb_touch = any(float(l) <= float(b) for l, b in zip(recent_lows, boll_lowers) if not pd.isna(b))
        if bb_touch: power_score = int(min(100, power_score + 15))

        cap_vol = detect_capitulation_volume(df)
        if cap_vol: power_score = int(min(100, power_score + 20))

        signals = ["RSI Dönüş"]
        if volume_now > vol_avg10 * 1.5: signals.append("Yüksek Hacim")
        if close_price > sma50: signals.append("SMA50 Üstü")
        if close_price > ema50: signals.append("EMA50 Üstü")
        if not pd.isna(macd_now) and float(macd_now) > float(macd_signal_now): signals.append("MACD +")
        if weekly_info["bullish"]: signals.append("Haftalık ✓")
        if rsi_div: signals.append("RSI Uyumsuzluk")
        if bb_touch: signals.append("BB Alt ✓")
        if cap_vol: signals.append("Teslim Hacmi")
        if cdl_label and cdl_label != "Doji": signals.append(cdl_label)

        results.append({
            "Hisse": ticker.replace(".IS", ""), "Güç Skoru": power_score, "RSI": round(float(rsi_now), 1),
            "Sinyaller": " | ".join(signals), "Giriş ₺": round(entry, 2), "Hedef ₺": round(target, 2),
            "Hedef %": round(target_pct, 1), "Stop ₺": round(stop, 2), "Stop %": round(stop_pct, 1),
            "Günlük TL Hacim": round(vol_tl_avg10), "_sector": BIST100_SECTORS.get(ticker, ""),
        })

    sector_counts: dict[str, int] = {}
    for r in results:
        s = r.get("_sector", "")
        if s: sector_counts[s] = sector_counts.get(s, 0) + 1
    for r in results:
        sec = r.pop("_sector", "")
        if sec and sector_counts.get(sec, 0) >= 2:
            r["Güç Skoru"] = min(100, r["Güç Skoru"] + 10)
            if "Sektör Uyumu" not in r["Sinyaller"]: r["Sinyaller"] += " | Sektör Uyumu"
    return results

def send_whatsapp(phone: str, apikey: str, results: list[dict], label: str = "") -> bool:
    header = f"📈 BIST100 {label} ({datetime.now(TZ_ISTANBUL).strftime('%d.%m.%Y %H:%M')}):"
    lines = [header]
    for i, row in enumerate(results[:10], 1):
        lines.append(f"{i}. {row['Hisse']} - Giriş: {row['Giriş ₺']} ₺, Hedef: {row['Hedef ₺']} ₺, Stop: {row['Stop ₺']} ₺ [Güç: {row['Güç Skoru']}]")
    wa_text = urllib.parse.quote("\n".join(lines))
    try:
        resp = requests.get(f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={wa_text}&apikey={apikey}", timeout=15)
        return resp.status_code == 200
    except Exception: return False

def _bot_scan_job():
    with _bot_lock:
        if not _bot_state["active"]: return
        phone = _bot_state["phone"]
        apikey = _bot_state["apikey"]
        params = _bot_state["params"].copy()

    results = scan_tickers_core(BIST100_TICKERS, get_df_fn=download_data_intraday, rsi_min=params["rsi_min"], rsi_max=params["rsi_max"], sma_threshold=params["sma_threshold"], atr_multiplier=params["atr_multiplier"], rrr_multiplier=params["rrr_multiplier"], min_vol_tl=params.get("min_vol_tl", 50_000_000))
    now_str = datetime.now(TZ_ISTANBUL).strftime("%H:%M")
    with _bot_lock:
        _bot_state["last_run"] = datetime.now(TZ_ISTANBUL)
        _bot_state["last_count"] = len(results)
        _bot_state["last_label"] = f"Saatlik Tarama {now_str}"
    append_scan_log(results, source=f"Bot ({now_str})")
    if results and phone and apikey: send_whatsapp(phone, apikey, results, label=f"Saatlik Tarama {now_str}")

@st.cache_resource
def get_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=TZ_ISTANBUL)
    scan_times = [(9, 55), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (16, 0), (17, 0), (17, 50)]
    for hour, minute in scan_times:
        scheduler.add_job(_bot_scan_job, CronTrigger(hour=hour, minute=minute, timezone=TZ_ISTANBUL), id=f"bot_{hour:02d}{minute:02d}", replace_existing=True, misfire_grace_time=120)
    scheduler.start()
    return scheduler

st.title("📈 BIST100 Dipten Dönüş Tarayıcı")
st.markdown("RSI dönüş sinyali, hacim onayı, ATR tabanlı dinamik stop/hedef ve mum formasyonu analizi.")

_mkt_state = get_market_state()
if _mkt_state.get("bullish") is True:
    st.success(f"🟢 **BIST100 Endeks:** {_mkt_state['price']:,.0f} ₺ — EMA50 ({_mkt_state['ema50']:,.0f} ₺) üstünde (+{_mkt_state['pct']}%) · Genel trend olumlu")
elif _mkt_state.get("bullish") is False:
    st.error(f"🔴 **BIST100 Endeks:** {_mkt_state['price']:,.0f} ₺ — EMA50 ({_mkt_state['ema50']:,.0f} ₺) altında ({_mkt_state['pct']}%) · Piyasaya karşı işlem riski yüksek")

with st.sidebar:
    st.header("⚙️ Tarayıcı Ayarları")
    rsi_min = st.slider("RSI Alt Sınır", 20, 40, 30)
    rsi_max = st.slider("RSI Üst Sınır", 35, 55, 45)
    sma_threshold = st.slider("SMA50 Sapma Sınırı (%)", 5, 35, 20)
    atr_multiplier = st.slider("ATR Stop Çarpanı", 1.0, 3.0, 1.0, step=0.1)
    rrr_multiplier = st.slider("Risk/Getiri Oranı (RRR)", 1.5, 4.0, 2.0, step=0.25)
    min_vol_tl = st.select_slider("Min. Ort. Günlük Hacim (TL)", options=[10_000_000, 25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000], value=50_000_000, format_func=lambda x: f"{x // 1_000_000:.0f}M ₺")
    use_market_filter = st.checkbox("Endeks Filtresi (XU100 EMA50)", value=True)

_saved = load_settings()
with st.sidebar.expander("📱 WhatsApp Bildirim Ayarları"):
    wa_phone = st.text_input("Telefon Numarası", value=_saved["phone"], placeholder="Örn: 905XXXXXXXXX")
    wa_apikey = st.text_input("CallMeBot API Key", value=_saved["apikey"], type="password")
    bot_enabled = st.toggle("🤖 Otomatik Bot'u Etkinleştir", value=_saved["bot_enabled"])

    save_settings(wa_phone, wa_apikey, bot_enabled)
    _scheduler = get_scheduler()

    if bot_enabled and wa_phone and wa_apikey:
        with _bot_lock:
            _bot_state["active"] = True
            _bot_state["phone"] = wa_phone
            _bot_state["apikey"] = wa_apikey
            _bot_state["params"] = {"rsi_min": rsi_min, "rsi_max": rsi_max, "sma_threshold": sma_threshold, "atr_multiplier": atr_multiplier, "rrr_multiplier": rrr_multiplier, "min_vol_tl": min_vol_tl}
        st.success("Bot aktif!", icon="✅")
    else:
        with _bot_lock: _bot_state["active"] = False

with _bot_lock:
    _is_active = _bot_state["active"]
    _last_run = _bot_state["last_run"]
    _last_count = _bot_state["last_count"]

if _is_active:
    last_run_str = _last_run.strftime("%d.%m.%Y %H:%M") if _last_run else "henüz çalışmadı"
    st.info(f"🤖 **Otomatik Bot: Aktif** | Son tarama: {last_run_str} | Son sinyal sayısı: {_last_count}")

scan_col, refresh_col = st.columns([3, 1])
with scan_col: scan_btn = st.button("🔍 Taramayı Başlat", type="primary", use_container_width=True)
with refresh_col: clear_btn = st.button("🗑️ Önbelleği Temizle", use_container_width=True)

if clear_btn:
    st.cache_data.clear()
    st.success("Önbellek temizlendi!")

if scan_btn:
    progress_bar = st.progress(0)
    status_text = st.empty()
    def _progress_cb(ticker, current, total):
        status_text.text(f"Tarıyor: {ticker.replace('.IS', '')} ({current}/{total})")
        progress_bar.progress(current / total)

    raw_results = scan_tickers_core(BIST100_TICKERS, get_df_fn=download_data, rsi_min=rsi_min, rsi_max=rsi_max, sma_threshold=sma_threshold, atr_multiplier=atr_multiplier, rrr_multiplier=rrr_multiplier, min_vol_tl=min_vol_tl, progress_cb=_progress_cb)
    progress_bar.empty()
    status_text.empty()

    if raw_results:
        df_results = pd.DataFrame(raw_results).sort_values("Güç Skoru", ascending=False).reset_index(drop=True)
        df_results.index = df_results.index + 1
        st.success(f"✅ Tarama tamamlandı! {len(df_results)} hisse bulundu.")
        st.dataframe(df_results, use_container_width=True)
    else:
        st.warning("⚠️ Sinyal veren hisse bulunamadı.")

