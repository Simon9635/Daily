#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata

import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd

try:
    BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
except KeyError:
    BOT_TOKEN = ""
    CHAT_ID = ""

KST = dt.timezone(dt.timedelta(hours=9))
TG_MAX = 4096

# ---------- Telegram ----------
def tg_send(text: str):
    if not BOT_TOKEN: return
    def _post(msg: str):
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": msg, "disable_web_page_preview": True, "parse_mode": "HTML"}
        body = parse.urlencode(data).encode("utf-8")
        req = request.Request(url, data=body, method="POST")
        try: request.urlopen(req, timeout=30)
        except: pass
    
    # 긴 메시지 나누어 보내기
    if len(text) <= TG_MAX: _post(text)
    else:
        for i in range(0, len(text), TG_MAX): _post(text[i:i+TG_MAX])

# ---------- 날짜 선정 (진단용 로그 포함) ----------
def pick_compare_days(now_kst: dt.datetime) -> tuple:
    try:
        target_date = now_kst.date()
        # 넉넉하게 조회
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=20)).strftime("%Y%m%d")
        
        # 삼성전자 데이터로 거래일 확인
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        valid_dates = df.index.tolist()
        
        # [무조건] 가장 최근 마감일 2개 가져오기
        # (만약 오늘 장중이라면 오늘 데이터가 포함될 수 있음 -> 이 경우 오늘 vs 어제 비교됨)
        # (만약 아침이라면 어제 vs 그제 비교됨)
        d1 = valid_dates[-2].date()
        d0 = valid_dates[-3].date()
        
        return d1, d0
    except:
        return None, None

def yyyymmdd(d): return d.strftime("%Y%m%d")
def yyyy_mm_dd(d): return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 ----------
def get_data(datestr, market):
    try:
        df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
        if df is None or df.empty: return pd.DataFrame()
        df = df.reset_index()
        # 거래대금 컬럼 찾기
        col = next((c for c in df.columns if "대금" in c or "거래금액" in c), None)
        if not col: return pd.DataFrame()
        return df[["티커", col]].rename(columns={col: "대금"}).fillna(0).astype({"대금":"int64"})
    except:
        return pd.DataFrame()

def get_chart_status(ticker):
    try:
        now = dt.datetime.now(KST)
        df = fdr.DataReader(ticker, start=(now-dt.timedelta(days=100)).strftime("%Y-%m-%d"))
        if len(df) < 20: return "-"
        curr = df['Close'].iloc[-1]
        high = df['Close'].max()
        if curr >= high*0.97: return "🔥신고"
        if df['Close'].mean() < curr: return "📈상승"
        return "⚖️중립"
    except: return "-"

# ---------- 리포트 생성 (진단 모드) ----------
def build_report():
    now = dt.datetime.now(KST)
    d1, d0 = pick_compare_days(now)
    
    if not d1: return "❌ 날짜 계산 실패 (데이터 접속 불가)"
    
    # 데이터 수집
    dfs1, dfs0 = [], []
    for m in ["KOSPI", "KOSDAQ"]:
        dfs1.append(get_data(yyyymmdd(d1), m))
        dfs0.append(get_data(yyyymmdd(d0), m))
        
    v1 = pd.concat(dfs1)
    v0 = pd.concat(dfs0)
    
    if v1.empty or v0.empty:
        return f"❌ 데이터 수집 실패\n({d1} 또는 {d0} 데이터 없음)"

    # 병합 및 계산
    merged = pd.merge(v1, v0, on="티커", suffixes=("_1", "_0"))
    merged = merged[merged["대금_0"] > 10000000] # 1천만원 이상만 (노이즈 제거)
    merged["배수"] = merged["대금_1"] / merged["대금_0"]
    
    # [진단 핵심] 필터 없이 배수 높은 순으로 정렬
    top = merged.sort_values(by="배수", ascending=False).head(15)
    
    # 메시지 작성
    names = []
    for t in top["티커"]:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append(t)
        
    msg = [f"<b>[진단 모드: 거래대금 급증 Top 15]</b>"]
    msg.append(f"기준: {yyyy_mm_dd(d1)} vs {yyyy_mm_dd(d0)}")
    msg.append(f"전체 종목 수: {len(merged)}개 비교됨")
    msg.append("-" * 30)
    msg.append(f"`순위  종목명     배수   차트`")
    
    for i, (idx, row) in enumerate(top.iterrows(), 1):
        name = names[i-1][:5]
        ratio = f"{row['배수']:.1f}배"
        chart = get_chart_status(row['티커'])
        # 가운데 정렬 흉내
        line = f"{i}) {name:<5} {ratio:>5} {chart}"
        msg.append(f"<code>{line}</code>")
        
    msg.append("-" * 30)
    msg.append("※ 5배(5.0) 이상인 종목이 없으면 '해당 없음'이 뜹니다.")
    
    return "\n".join(msg)

if __name__ == "__main__":
    try:
        msg = build_report()
        tg_send(msg)
    except Exception as e:
        tg_send(f"FATAL ERROR: {e}")


