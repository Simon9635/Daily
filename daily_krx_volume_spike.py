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
import re
import FinanceDataReader as fdr  # 차트 분석용
from pykrx import stock          # 거래대금/시총용
import pandas as pd

# --- Telegram ENV ---
# Github Actions Secrets에서 환경변수를 불러옵니다.
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
    """메시지 길이가 길면 나누어 전송하고, HTML 파싱 에러 시 일반 텍스트로 재전송"""
    if not BOT_TOKEN:
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
        try:
            _post(text, parse_html=True)
        except:
            _post(text, parse_html=False)
        return

    i = 0
    while i < len(text):
        chunk = text[i:i+TG_MAX]
        try:
            _post(chunk, parse_html=True)
        except:
            _post(chunk, parse_html=False)
        i += TG_MAX

# ---------- [핵심] 날짜 계산 (휴장일 자동 건너뛰기) ----------
def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    [수정됨] 오늘(T)이 아닌, '직전 거래일(T-1)'과 '그 전 거래일(T-2)'을 비교합니다.
    예: 오늘이 7일이면 -> 6일 vs 5일(또는 3일) 비교
    """
    try:
        target_date = now_kst.date()
        
        # 넉넉하게 2주치 조회
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        # 삼성전자 일별 시세로 거래일 확인
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        # 최소 3일치 데이터가 있어야 T-1 vs T-2 비교 가능
        if df.empty or len(df) < 3:
            return None, None
            
        valid_dates = df.index.tolist()
        
        # [핵심 수정] 인덱스 변경
        # 원래: d1=valid_dates[-1] (오늘), d0=valid_dates[-2] (어제)
        # 변경: d1=valid_dates[-2] (어제), d0=valid_dates[-3] (그저께)
        d1 = valid_dates[-2].date() 
        d0 = valid_dates[-3].date() 
        
        return d1, d0
    except Exception:
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 (pykrx) ----------
def get_trading_value_by_market(datestr: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["티커", "거래대금", "시장"])
        df = df.reset_index()
        val_col = "거래대금" if "거래대금" in df.columns else next(
            (c for c in df.columns if "대금" in c), None
        )
        if not val_col:
            return pd.DataFrame(columns=["티커", "거래대금", "시장"])
        out = df[["티커", val_col]].copy()
        out.rename(columns={val_col: "거래대금"}, inplace=True)
        out["거래대금"] = pd.to_numeric(out["거래대금"], errors="coerce")
        out.dropna(subset=["거래대금"], inplace=True)
        out["거래대금"] = out["거래대금"].astype("int64")
        out["시장"] = market
        return out
    except:
        return pd.DataFrame(columns=["티커", "거래대금", "시장"])

def get_mcap_by_market(datestr: str, market: str) -> pd.DataFrame:
    try:
        df = stock.get_market_cap_by_ticker(datestr, market=market)
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["티커", "시가총액"])
        df = df.reset_index()
        cap_col = "시가총액" if "시가총액" in df.columns else next((c for c in df.columns if "총액" in c), None)
        if not cap_col:
            return pd.DataFrame(columns=["티커", "시가총액"])
        out = df[["티커", cap_col]].copy()
        out.rename(columns={cap_col: "시가총액"}, inplace=True)
        out["시가총액"] = pd.to_numeric(out["시가총액"], errors="coerce")
        out.dropna(subset=["시가총액"], inplace=True)
        return out
    except:
        return pd.DataFrame(columns=["티커", "시가총액"])

# ---------- 차트 분석 (FinanceDataReader) ----------
def get_chart_status(ticker: str) -> str:
    """FinanceDataReader를 사용하여 안정적으로 차트 위치(신고가, 정배열 등) 분석"""
    try:
        now = dt.datetime.now(KST)
        today = now.strftime("%Y-%m-%d")
        start_day = (now - dt.timedelta(days=120)).strftime("%Y-%m-%d")
        
        # fdr 사용 (pykrx 컬럼 에러 방지)
        df = fdr.DataReader(ticker, start=start_day, end=today)
        
        if df.empty or len(df) < 60:
            return "-"

        close = df['Close']
        current_price = close.iloc[-1]
        
        # 1. 신고가 (최근 60일 고가 기준)
        recent_high = close.max()
        if current_price >= recent_high * 0.98:
            return "🔥신고가"

        # 이평선
        ma5 = close.rolling(window=5).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        ma60 = close.rolling(window=60).mean().iloc[-1]

        # 2. 역배열
        if ma60 > ma20 > ma5:
            return "📉역배열"

        # 3. 박스권/바닥권
        recent_low = close.min()
        if recent_high > recent_low:
            pos = (current_price - recent_low) / (recent_high - recent_low)
            if pos >= 0.8:
                return "📦박스상단"
            elif pos <= 0.2:
                return "💧바닥권"

        # 4. 정배열
        if ma5 > ma20 > ma60:
            return "📈정배열"

        return "⚖️중립"
    except Exception:
        return "-"

# ---------- 리포트 생성 (가운데 정렬 + 테이블) ----------
def build_report():
    
    # [1] 글자 폭 계산 (한글=2, 영어=1)
    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    # [2] 가운데 정렬 함수
    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width:
            return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    
    # 휴장일이거나 데이터가 없으면 종료
    if d1_date is None:
        return None 

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # 데이터 수집 (코스피+코스닥)
    vals_d1, vals_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vals_d1.append(get_trading_value_by_market(d1_str, mkt))
        vals_d0.append(get_trading_value_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))

    val1 = pd.concat(vals_d1, ignore_index=True) if vals_d1 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    val0 = pd.concat(vals_d0, ignore_index=True) if vals_d0 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커", "시가총액"])

    # 필터링 (거래대금 5배 급증)
    merged = pd.merge(val1, val0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래대금_전일", "거래대금_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래대금_전일", "거래대금_전전일"])
    merged = merged[merged["거래대금_전전일"] > 0]
    merged["배수"] = (merged["거래대금_전일"] / merged["거래대금_전전일"]).round(2)

    result = merged[merged["배수"] >= 5].copy()

    # 정렬 (시총순 -> 대금순)
    result = pd.merge(result, mcap, on="티커", how="left")
    result["시가총액"] = pd.to_numeric(result["시가총액"], errors="coerce").fillna(0)
    result.sort_values(by=["시가총액", "거래대금_전일"], ascending=[False, False], inplace=True)
    result = result.head(30).reset_index(drop=True)

    # 종목명 가져오기
    name_map = {}
    for t in result["티커"].tolist():
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # 포맷팅 준비
    amts = []
    for v in result["거래대금_전일"].tolist():
        v_eok = int(v) / 100_000_000
        amts.append(f"{v_eok:,.1f}")

    names = [str(x or "") for x in result["종목명"].tolist()]
    tickers = result["티커"].tolist()

    # 헤더 작성
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 sk 김수민입니다\n"
        f"<b>전일거래대금 급증 종목 공유드립니다!</b>\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    # ===== [설정] 테이블 칼럼 너비 =====
    W_RANK = 3
    W_NAME = 13   # 종목명 폭
    W_AMT  = 11   # 거래대금 폭
    W_CHART= 11   # 차트 폭
    GAP    = "  " # 칼럼 간 간격

    # 테이블 헤더
    h_rank = " " * W_RANK
    h_name = center_align("종목", W_NAME)
    h_amt  = center_align("대금", W_AMT)
    h_chart= center_align("차트", W_CHART)
    
    header_line = f"{h_rank}{GAP}{h_name}{GAP}{h_amt}{GAP}{h_chart}"
    lines = [f"<code>{html.escape(header_line)}</code>"]
    lines.append("-" * 45)

    # 테이블 본문
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), start=1):
        # 1. 순위 (왼쪽 정렬)
        rank_str = f"{str(i)+')':<{W_RANK}}"
        
        # 2. 종목 (5글자 컷 + 가운데 정렬)
        nm_trunc = nm[:5] 
        name_str = center_align(nm_trunc, W_NAME)
        
        # 3. 대금 (가운데 정렬)
        amt_str = center_align(av, W_AMT)
        
        # 4. 차트 (분석 + 가운데 정렬)
        status = get_chart_status(t_code)
        chart_str = center_align(status, W_CHART)
        
        row_str = f"{rank_str}{GAP}{name_str}{GAP}{amt_str}{GAP}{chart_str}"
        lines.append(f"<code>{html.escape(row_str)}</code>")

    return header + "\n" + "\n".join(lines)

# ---------- 메인 실행부 ----------
if __name__ == "__main__":
    try:
        msg = build_report()
        if msg:
            tg_send(msg)
        else:
            print("생성된 메시지 없음 (휴장일 또는 조건 만족 종목 없음)")
    except Exception as e:
        print(f"Error: {e}")
        try:
            tg_send(f"⚠️ 자동화 에러: {e}")
        except:
            pass
        sys.exit(1)
