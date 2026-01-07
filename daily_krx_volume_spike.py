#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata

# --- [필수 라이브러리] ---
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr  # 차트 분석용
from pykrx import stock          # 거래대금/시총용
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
    
    def _post(msg: str, parse_html: bool = True):
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": CHAT_ID,
            "text": msg,
            "disable_web_page_preview": True,
        }
        if parse_html:
            data["parse_mode"] = "HTML"
        body = parse.urlencode(data).encode("utf-8")
        req = request.Request(url, data=body, method="POST")
        try:
            with request.urlopen(req, timeout=30) as resp:
                json.loads(resp.read().decode("utf-8"))
        except Exception:
            pass

    if len(text) <= TG_MAX:
        try: _post(text, True)
        except: _post(text, False)
        return

    i = 0
    while i < len(text):
        chunk = text[i:i+TG_MAX]
        try: _post(chunk, True)
        except: _post(chunk, False)
        i += TG_MAX

# ---------- [핵심] 날짜 계산 (주말 자동 스킵) ----------
def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    DB에 존재하는 '가장 최근 거래일(d1)'과 '그 직전 거래일(d0)'을 반환.
    리스트 기반이므로 월요일엔 자동으로 금요일과 비교하게 됨.
    """
    try:
        target_date = now_kst.date()
        
        # 넉넉하게 2주치 조회
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        if df.empty or len(df) < 2:
            return None, None
            
        valid_dates = df.index.tolist()
        
        # [수정됨] 무조건 가장 최근 데이터 2개 추출
        d1 = valid_dates[-1].date() # 예: 1/5 (월)
        d0 = valid_dates[-2].date() # 예: 1/2 (금)
        
        return d1, d0
    except Exception:
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 ----------
def get_trading_value_by_market(datestr: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["티커", "거래대금"])
        df = df.reset_index()
        val_col = next((c for c in df.columns if "대금" in c or "거래금액" in c), None)
        if not val_col:
            return pd.DataFrame(columns=["티커", "거래대금"])
        out = df[["티커", val_col]].copy()
        out.rename(columns={val_col: "거래대금"}, inplace=True)
        out["거래대금"] = pd.to_numeric(out["거래대금"], errors="coerce").fillna(0).astype("int64")
        return out
    except:
        return pd.DataFrame(columns=["티커", "거래대금"])

def get_mcap_by_market(datestr: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_cap_by_ticker(datestr, market=market)
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["티커", "시가총액"])
        df = df.reset_index()
        cap_col = next((c for c in df.columns if "총액" in c), None)
        if not cap_col:
            return pd.DataFrame(columns=["티커", "시가총액"])
        out = df[["티커", cap_col]].copy()
        out.rename(columns={cap_col: "시가총액"}, inplace=True)
        out["시가총액"] = pd.to_numeric(out["시가총액"], errors="coerce").fillna(0)
        return out
    except:
        return pd.DataFrame(columns=["티커", "시가총액"])

# ---------- 차트 분석 (FinanceDataReader) ----------
def get_chart_status(ticker: str) -> str:
    try:
        now = dt.datetime.now(KST)
        today = now.strftime("%Y-%m-%d")
        start_day = (now - dt.timedelta(days=120)).strftime("%Y-%m-%d")
        
        df = fdr.DataReader(ticker, start=start_day, end=today)
        if df.empty or len(df) < 60:
            return "-"

        close = df['Close']
        curr = close.iloc[-1]
        
        # 1. 신고가 (최근 60일 고가 기준)
        recent_high = close.tail(60).max()
        if curr >= recent_high * 0.98:
            return "🔥신고가"

        # 이평선
        ma5 = close.rolling(5).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]

        # 2. 역배열
        if ma60 > ma20 > ma5:
            return "📉역배열"

        # 3. 박스권/바닥권
        recent_low = close.tail(60).min()
        if recent_high > recent_low:
            pos = (curr - recent_low) / (recent_high - recent_low)
            if pos >= 0.8: return "📦박스상단"
            elif pos <= 0.2: return "💧바닥권"

        # 4. 정배열
        if ma5 > ma20 > ma60:
            return "📈정배열"

        return "⚖️중립"
    except:
        return "-"

# ---------- 리포트 생성 (요청하신 포맷 적용) ----------
def build_report():
    import unicodedata, html

    # [글자 폭 계산]
    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    # [가운데 정렬 함수]
    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width: return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    
    if d1_date is None:
        return None

    d1_str = yyyymmdd(d1_date)
    d0_str = yyyymmdd(d0_date)

    # 데이터 수집
    dfs_v1, dfs_v0, dfs_cap = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        dfs_v1.append(get_trading_value_by_market(d1_str, mkt))
        dfs_v0.append(get_trading_value_by_market(d0_str, mkt))
        dfs_cap.append(get_mcap_by_market(d1_str, mkt))

    v1 = pd.concat(dfs_v1, ignore_index=True)
    v0 = pd.concat(dfs_v0, ignore_index=True)
    cap = pd.concat(dfs_cap, ignore_index=True)

    if v1.empty or v0.empty:
        return None

    # [필터링] 거래대금 급증 5배(500%) 이상
    merged = pd.merge(v1, v0, on="티커", how="inner", suffixes=("_1", "_0"))
    merged = merged[merged["거래대금_0"] > 0]
    merged["배수"] = merged["거래대금_1"] / merged["거래대금_0"]
    
    target = merged[merged["배수"] >= 5.0].copy()

    # 정렬 및 Top 30
    target = pd.merge(target, cap, on="티커", how="left").fillna(0)
    target = target.sort_values(by="거래대금_1", ascending=False).head(30)

    # 포맷팅용 데이터 준비
    names = []
    tickers = target["티커"].tolist()
    for t in tickers:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append("-")

    amts = []
    for val in target["거래대금_1"].tolist():
        amts.append(f"{val/100000000:,.1f}")

    # ===== 메시지 구성 =====
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 \n"
        f"sk 김수민입니다\n"
        f"전일거래대금 급증 종목 공유드립니다!\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    # [테이블 너비 설정]
    W_NUM = 3    # "1) "
    W_NAME = 12  # 종목명
    W_AMT = 12   # 거래대금
    W_CHART = 10 # 차트
    GAP = "   "  # 칼럼 사이 공백

    # 헤더 라인 (가운데 정렬)
    h_line = (
        f"{' '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align('거래대금(억)', W_AMT)}{GAP}"
        f"{center_align('차트', W_CHART)}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 48) # 구분선 길이

    # 데이터 라인 생성
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME) # 5글자 제한
        amt_s = center_align(av, W_AMT)
        stat = get_chart_status(t_code)
        chart_s = center_align(stat, W_CHART)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}{GAP}{chart_s}"
        lines.append(f"<code>{html.escape(row)}</code>")

    return header + "\n".join(lines)

# ---------- 실행부 ----------
if __name__ == "__main__":
    try:
        msg = build_report()
        if msg:
            tg_send(msg)
        else:
            print("생성된 메시지 없음")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
