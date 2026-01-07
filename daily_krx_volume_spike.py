#!/usr/bin/env python3
import os
import sys
import time
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata

# --- [필수 라이브러리] ---
import requests
import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd

# --- Telegram ENV ---
try:
    BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
except KeyError:
    BOT_TOKEN = ""
    CHAT_ID = ""

KST = dt.timezone(dt.timedelta(hours=9))
TG_MAX = 4096

# ---------- Telegram 전송 함수 ----------
def tg_send(text: str):
    if not BOT_TOKEN:
        print("❌ 봇 토큰 없음")
        return
    
    def _post(msg: str):
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": CHAT_ID,
            "text": msg,
            "disable_web_page_preview": True,
            "parse_mode": "HTML"
        }
        body = parse.urlencode(data).encode("utf-8")
        req = request.Request(url, data=body, method="POST")
        try:
            with request.urlopen(req, timeout=30) as resp:
                json.loads(resp.read().decode("utf-8"))
        except: pass

    if len(text) <= TG_MAX:
        _post(text)
    else:
        for i in range(0, len(text), TG_MAX):
            _post(text[i:i+TG_MAX])

# ---------- 날짜 자동 보정 ----------
def pick_latest_valid_days() -> tuple[dt.date, dt.date]:
    """2024년부터 조회하여 실제 데이터가 있는 마지막 날짜를 찾습니다."""
    try:
        now = dt.datetime.now(KST)
        start_str = "20240101"
        end_str = now.strftime("%Y%m%d")
        
        # 삼성전자 기준으로 거래일 확인
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        if df.empty or len(df) < 2: return None, None
            
        valid_dates = df.index.tolist()
        return valid_dates[-1].date(), valid_dates[-2].date()
    except:
        return None, None

def yyyymmdd(d): return d.strftime("%Y%m%d")
def yyyy_mm_dd(d): return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 ----------
def get_market_data(datestr, market):
    """안정적인 get_market_cap_by_ticker 사용"""
    for i in range(3):
        try:
            time.sleep(1)
            df = stock.get_market_cap_by_ticker(datestr, market=market)
            if df is None or len(df) == 0: continue

            df = df.reset_index()
            # 거래대금 컬럼 찾기
            val_col = next((c for c in df.columns if "대금" in c or "거래금액" in c), None)
            if not val_col: continue

            out = df[["티커", val_col]].copy()
            out.rename(columns={val_col: "거래대금"}, inplace=True)
            out["거래대금"] = pd.to_numeric(out["거래대금"]).fillna(0).astype("int64")
            return out
        except:
            time.sleep(2)
    return pd.DataFrame(columns=["티커", "거래대금"])

# ---------- 차트 분석 ----------
def get_chart_status(ticker):
    try:
        now = dt.datetime.now(KST)
        today = now.strftime("%Y-%m-%d")
        start = (now - dt.timedelta(days=100)).strftime("%Y-%m-%d")
        df = fdr.DataReader(ticker, start=start, end=today)
        if len(df) < 20: return "-"
        
        curr = df['Close'].iloc[-1]
        high = df['Close'].max()
        
        if curr >= high * 0.98: return "🔥신고"
        if df['Close'].mean() < curr: return "📈상승"
        return "⚖️중립"
    except: return "-"

# ---------- 리포트 생성 (필터 해제) ----------
def build_report():
    import unicodedata

    def disp_width(s): 
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)
    def center(s, w):
        sl = disp_width(s)
        if sl >= w: return s
        pad = w - sl
        return " "*(pad//2) + s + " "*(pad - pad//2)

    d1, d0 = pick_latest_valid_days()
    if not d1: return "❌ 날짜 분석 실패"

    print(f"⏳ 데이터 수집: {d1} vs {d0}")
    
    dfs1, dfs0 = [], []
    for m in ["KOSPI", "KOSDAQ"]:
        dfs1.append(get_market_data(yyyymmdd(d1), m))
        dfs0.append(get_market_data(yyyymmdd(d0), m))

    v1 = pd.concat(dfs1)
    v0 = pd.concat(dfs0)

    if v1.empty: return "❌ 데이터 수집 실패 (빈 데이터)"

    # 병합
    m = pd.merge(v1, v0, on="티커", suffixes=("_1", "_0"))
    
    # [진단] 거래대금 1천만원 이상만 (노이즈 제거)
    m = m[m["거래대금_0"] > 10000000] 
    m["배수"] = m["거래대금_1"] / m["거래대금_0"]

    # [수정됨] 5배 필터 제거 -> 그냥 상위 15개 추출
    target = m.sort_values(by="배수", ascending=False).head(15)

    # 메시지 작성
    names = []
    for t in target["티커"]:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append(t)

    header = (
        f"<b>[진단 모드: 급증률 상위 15위]</b>\n"
        f"기준: {yyyy_mm_dd(d1)} vs {yyyy_mm_dd(d0)}\n"
        f"※ 5배 제한 없이 무조건 보여줍니다.\n"
    )

    W_NM, W_RT, W_CH = 10, 8, 8
    
    lines = ["-"*35]
    lines.append(f"`순위  종목      배수    차트`")
    
    for i, (idx, row) in enumerate(target.iterrows(), 1):
        nm = center(names[i-1][:4], W_NM)
        rt = center(f"{row['배수']:.1f}배", W_RT)
        ch = center(get_chart_status(row['티커']), W_CH)
        lines.append(f"<code>{i:<3} {nm} {rt} {ch}</code>")
        
    return header + "\n".join(lines)

if __name__ == "__main__":
    print("🚀 진단 시작")
    try:
        msg = build_report()
        tg_send(msg)
        print("✅ 전송 완료")
    except Exception as e:
        print(f"❌ 에러: {e}")
