#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata
import time

# --- [필수 라이브러리] ---
import requests
from bs4 import BeautifulSoup
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
        print("❌ [오류] 봇 토큰이 설정되지 않았습니다. 메시지를 보낼 수 없습니다.")
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
            print(f"❌ [전송 실패] 텔레그램 API 에러: {e}")

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

# ---------- [핵심] 날짜 계산 (안전장치 추가) ----------
def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    DB에 있는 '가장 최근 거래일'을 기준으로 비교 날짜를 선정합니다.
    [중요] 만약 조회된 마지막 날짜가 '오늘'이라면, 아직 장 마감 데이터가
    완전하지 않을 수 있으므로 안전하게 '어제' vs '그제'로 미룹니다.
    """
    try:
        target_date = now_kst.date()
        print(f"🔎 [날짜분석] 오늘 날짜: {target_date}")
        
        # 넉넉하게 2주치 조회
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        # 삼성전자 데이터로 거래일 확인
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        if df.empty or len(df) < 3:
            print("❌ [날짜분석] 최근 거래일 데이터를 불러올 수 없습니다.")
            return None, None
            
        valid_dates = df.index.tolist()
        last_db_date = valid_dates[-1].date()
        print(f"🔎 [날짜분석] DB상 최근 거래일: {last_db_date}")
        
        # [안전장치] DB 마지막 날짜가 '오늘'이면 -> 하루 전으로 이동
        # (이유: 장 중이거나 데이터 집계 전일 수 있으므로 확정된 어제 데이터를 쓰기 위함)
        if last_db_date == target_date:
            print("💡 [날짜조정] 오늘 데이터가 감지되어 '어제' 마감 기준으로 변경합니다.")
            d1 = valid_dates[-2].date()
            d0 = valid_dates[-3].date()
        else:
            # DB 마지막 날짜가 오늘이 아님 (이미 어제 마감된 데이터임)
            d1 = valid_dates[-1].date()
            d0 = valid_dates[-2].date()
            
        print(f"✅ [최종선정] 기준일(T): {d1} vs 대조일(T-1): {d0}")
        return d1, d0
    except Exception as e:
        print(f"❌ [날짜분석 에러] {e}")
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- 데이터 수집 ----------
# [수정됨] 재시도 로직과 딜레이가 추가된 데이터 수집 함수
def get_trading_value_by_market(datestr: str, market: str) -> pd.DataFrame:
    """
    3번까지 재시도하며 데이터를 가져옵니다.
    연속 호출 시 차단을 막기 위해 1초 딜레이를 줍니다.
    """
    max_retries = 3
    for i in range(max_retries):
        try:
            # 1초 쉬고 요청 (차단 방지)
            time.sleep(1)
            
            df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
            
            if df is None or len(df) == 0:
                raise ValueError("빈 데이터 반환됨")
                
            df = df.reset_index()
            # 컬럼명 유연하게 찾기
            val_col = next((c for c in df.columns if "대금" in c or "거래금액" in c), None)
            if not val_col:
                return pd.DataFrame(columns=["티커", "거래대금"])
                
            out = df[["티커", val_col]].copy()
            out.rename(columns={val_col: "거래대금"}, inplace=True)
            out["거래대금"] = pd.to_numeric(out["거래대금"], errors="coerce").fillna(0).astype("int64")
            
            # 성공하면 즉시 반환
            return out
            
        except Exception as e:
            print(f"⚠️ [재시도 {i+1}/{max_retries}] {market} {datestr} 데이터 수집 실패: {e}")
            time.sleep(2) # 실패 시 2초 대기 후 재시도

    # 3번 다 실패하면 빈 데이터프레임 반환
    print(f"❌ 최종 실패: {market} {datestr} 데이터를 가져오지 못했습니다.")
    return pd.DataFrame(columns=["티커", "거래대금"])

# ---------- 차트 분석 ----------
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
    import unicodedata, html

    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

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

    print("⏳ 데이터 수집 중...")
    dfs_v1, dfs_v0, dfs_cap = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        dfs_v1.append(get_trading_value_by_market(d1_str, mkt))
        dfs_v0.append(get_trading_value_by_market(d0_str, mkt))
        dfs_cap.append(get_mcap_by_market(d1_str, mkt))

    v1 = pd.concat(dfs_v1, ignore_index=True)
    v0 = pd.concat(dfs_v0, ignore_index=True)
    cap = pd.concat(dfs_cap, ignore_index=True)

    if v1.empty or v0.empty:
        print("❌ [데이터 오류] 해당 날짜의 거래대금 데이터를 가져오지 못했습니다.")
        return None

    # [필터링]
    merged = pd.merge(v1, v0, on="티커", how="inner", suffixes=("_1", "_0"))
    merged = merged[merged["거래대금_0"] > 0]
    merged["배수"] = merged["거래대금_1"] / merged["거래대금_0"]
    
    target = merged[merged["배수"] >= 5.0].copy()
    print(f"📊 1차 필터링 완료: {len(target)}개 종목 발견")

    target = pd.merge(target, cap, on="티커", how="left").fillna(0)
    target = target.sort_values(by="거래대금_1", ascending=False).head(30)

    # 포맷팅
    names = []
    tickers = target["티커"].tolist()
    for t in tickers:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append("-")

    amts = []
    for val in target["거래대금_1"].tolist():
        amts.append(f"{val/100000000:,.1f}")

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
    if not BOT_TOKEN:
        print("⚠️ [경고] 봇 토큰이 없습니다. Secrets 설정을 확인하세요.")
    
    try:
        msg = build_report()
        if msg:
            print("📨 메시지 전송 시도...")
            tg_send(msg)
            print("✅ 전송 완료")
        else:
            print("⚠️ 생성된 메시지가 없습니다. (휴장일 또는 데이터 부족)")
    except Exception as e:
        print(f"❌ [치명적 에러] {e}")
        sys.exit(1)


