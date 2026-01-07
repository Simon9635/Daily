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
import FinanceDataReader as fdr  # 차트 분석용
from pykrx import stock          # 데이터 수집용
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
        print("❌ [설정 오류] 봇 토큰이 없습니다.")
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
        except Exception as e:
            print(f"❌ 전송 실패: {e}")

    if len(text) <= TG_MAX:
        try: _post(text, True)
        except: _post(text, False)
    else:
        for i in range(0, len(text), TG_MAX):
            chunk = text[i:i+TG_MAX]
            try: _post(chunk, True)
            except: _post(chunk, False)

# ---------- [핵심] 실제 데이터 존재 날짜 찾기 ----------
def pick_latest_valid_days() -> tuple[dt.date, dt.date]:
    """
    시스템 날짜(2026년 등)를 맹신하지 않고,
    '실제로 주가 데이터가 존재하는 가장 마지막 날짜'를 찾아냅니다.
    """
    try:
        now = dt.datetime.now(KST)
        print(f"🖥️ [시스템 기준 오늘] {now.date()}")
        
        # [해결책] 2024년부터 오늘까지의 데이터 중 '데이터가 있는 날'만 가져옴
        # 이렇게 하면 중간에 빈 날짜(휴일, 미래 등)는 자동으로 걸러집니다.
        start_str = "20240101"
        end_str = now.strftime("%Y%m%d")
        
        print("🔎 유효한 거래일 데이터를 검색합니다...")
        # 삼성전자(005930) 기준으로 거래일 리스트 확보
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        if df.empty or len(df) < 2:
            print("❌ [오류] 조회 가능한 거래일이 없습니다. (네이버 금융 접속 불가 등)")
            return None, None
            
        valid_dates = df.index.tolist()
        
        # 가장 마지막 날짜 = 진짜 최신 거래일
        real_d1 = valid_dates[-2].date() # 최신일
        real_d0 = valid_dates[-3].date() # 직전일
        
        print(f"✅ [날짜 확정] 데이터가 있는 최신일: {real_d1} (대조: {real_d0})")
        return real_d1, real_d0
        
    except Exception as e:
        print(f"❌ [날짜 분석 실패] {e}")
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 (안정성 강화) ----------
def get_market_data(datestr: str, market: str) -> pd.DataFrame:
    """
    get_market_cap_by_ticker를 사용하여 데이터 수집 (OHLCV보다 안정적)
    """
    max_retries = 3
    for i in range(max_retries):
        try:
            time.sleep(1) # 1초 대기 (차단 방지)
            
            # 시가총액/거래대금 등 전체 데이터 조회
            df = stock.get_market_cap_by_ticker(datestr, market=market)
            
            if df is None or len(df) == 0:
                raise ValueError("데이터 없음 (Empty)")

            df = df.reset_index()
            cols = df.columns
            
            # 컬럼명 유연하게 찾기
            val_col = next((c for c in cols if "대금" in c or "거래금액" in c), None)
            
            if not val_col:
                raise ValueError("거래대금 컬럼을 찾을 수 없음")

            # 필요한 컬럼만 추출
            out = df[["티커", val_col]].copy()
            out.rename(columns={val_col: "거래대금"}, inplace=True)
            out["거래대금"] = pd.to_numeric(out["거래대금"], errors="coerce").fillna(0).astype("int64")
            
            # 시가총액도 있으면 가져오기 (정렬용)
            cap_col = next((c for c in cols if "총액" in c), None)
            if cap_col:
                out["시가총액"] = df[cap_col]
            else:
                out["시가총액"] = 0
                
            return out

        except Exception as e:
            print(f"⚠️ [재시도 {i+1}] {market} {datestr} 읽기 실패: {e}")
            time.sleep(2)
            
    print(f"❌ [최종 실패] {market} {datestr} 데이터를 가져오지 못했습니다.")
    return pd.DataFrame(columns=["티커", "거래대금", "시가총액"])

# ---------- 차트 분석 ----------
def get_chart_status(ticker: str) -> str:
    try:
        # 차트 데이터 조회 (fdr은 날짜 범위가 넓어도 알아서 처리해줌)
        now = dt.datetime.now(KST)
        today_str = now.strftime("%Y-%m-%d")
        start_str = (now - dt.timedelta(days=120)).strftime("%Y-%m-%d")
        
        df = fdr.DataReader(ticker, start=start_str, end=today_str)
        if df.empty or len(df) < 60:
            return "-"

        close = df['Close']
        curr = close.iloc[-1]
        
        recent_high = close.tail(60).max()
        if curr >= recent_high * 0.98: return "🔥신고가"

        ma5 = close.rolling(5).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]

        if ma60 > ma20 > ma5: return "📉역배열"

        recent_low = close.tail(60).min()
        if recent_high > recent_low:
            pos = (curr - recent_low) / (recent_high - recent_low)
            if pos >= 0.8: return "📦박스상단"
            elif pos <= 0.2: return "💧바닥권"

        if ma5 > ma20 > ma60: return "📈정배열"

        return "⚖️중립"
    except:
        return "-"

# ---------- 리포트 생성 ----------
def build_report():
    import unicodedata

    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width: return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    # 1. 날짜 선정 (자동 보정)
    d1_date, d0_date = pick_latest_valid_days()
    
    if d1_date is None:
        return None

    d1_str = yyyymmdd(d1_date)
    d0_str = yyyymmdd(d0_date)

    print(f"⏳ 데이터 수집 시작 ({d1_str} vs {d0_str})...")
    
    dfs_1, dfs_0 = [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        dfs_1.append(get_market_data(d1_str, mkt))
        dfs_0.append(get_market_data(d0_str, mkt))

    df_1 = pd.concat(dfs_1, ignore_index=True)
    df_0 = pd.concat(dfs_0, ignore_index=True)

    if df_1.empty or df_0.empty:
        print("❌ [중단] 데이터가 비어있습니다. (해당 날짜 데이터 수집 불가)")
        return None

    # 데이터 병합
    df_1_sub = df_1[["티커", "거래대금", "시가총액"]]
    df_0_sub = df_0[["티커", "거래대금"]]
    
    merged = pd.merge(df_1_sub, df_0_sub, on="티커", how="inner", suffixes=("_1", "_0"))
    
    # [필터링] 거래대금 5배(500%) 이상
    merged = merged[merged["거래대금_0"] > 0]
    merged["배수"] = merged["거래대금_1"] / merged["거래대금_0"]
    
    target = merged[merged["배수"] >= 5.0].copy()
    print(f"📊 조건 만족 종목: {len(target)}개")

    # 정렬 (거래대금순)
    target = target.sort_values(by="거래대금_1", ascending=False).head(30)

    # 종목명 및 포맷팅
    names = []
    tickers = target["티커"].tolist()
    for t in tickers:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append(t)

    amts = []
    for val in target["거래대금_1"].tolist():
        amts.append(f"{val/100000000:,.1f}")

    # 메시지 작성
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 \n"
        f"sk 김수민입니다\n"
        f"전일거래대금 급증 종목 공유드립니다!\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    W_NUM = 3; W_NAME = 12; W_AMT = 12; W_CHART = 10; GAP = "   "

    h_line = (
        f"{' '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align('거래대금(억)', W_AMT)}{GAP}"
        f"{center_align('차트', W_CHART)}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 48)

    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME)
        amt_s = center_align(av, W_AMT)
        stat = get_chart_status(t_code)
        chart_s = center_align(stat, W_CHART)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}{GAP}{chart_s}"
        lines.append(f"<code>{html.escape(row)}</code>")

    return header + "\n".join(lines)

# ---------- 실행부 ----------
if __name__ == "__main__":
    print("🚀 스크립트 실행 시작")
    
    try:
        msg = build_report()
        if msg:
            print("📨 메시지 전송 시도...")
            tg_send(msg)
            print("✅ 전송 완료")
        else:
            print("⚠️ 보낼 메시지가 없습니다.")
    except Exception as e:
        print(f"❌ [치명적 에러] {e}")
        sys.exit(1)
