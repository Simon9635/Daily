#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
import time
from urllib import request, parse
import unicodedata

# --- [필수 라이브러리] ---
# pykrx 제거, fdr만 사용
import FinanceDataReader as fdr
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

# ---------- [핵심] FDR StockListing 활용 ----------
def get_market_data_fdr(market: str):
    """
    fdr.StockListing('KRX')를 사용하면 전 종목의
    [종가, 거래량, 거래대금, 등락률]을 한 번에 가져올 수 있습니다.
    웹 크롤링 방식보다 훨씬 안정적입니다.
    """
    try:
        print(f"📥 {market} 데이터 다운로드 중 (StockListing)...")
        # KRX: 코스피, 코스닥, 코넥스 통합 조회
        # KOSPI, KOSDAQ 별도 조회도 가능하지만 KRX로 한 번에 받는 게 효율적
        df = fdr.StockListing(market) # 'KOSPI' or 'KOSDAQ'
        
        if df is None or df.empty:
            return pd.DataFrame()
            
        # 필요한 컬럼만 선택 및 이름 통일
        # FDR 반환 컬럼: Code, Name, Close, Volume, Amount(거래대금), Marcap(시가총액) 등
        # (버전에 따라 컬럼명이 영어/한글 다를 수 있어 체크)
        cols = df.columns
        
        col_map = {
            'Code': '티커', 'Symbol': '티커',
            'Name': '종목명',
            'Close': '종가', 
            'Amount': '거래대금', 
            'Marcap': '시가총액'
        }
        df = df.rename(columns=col_map)
        
        # 필수 컬럼 존재 확인
        if '티커' not in df.columns or '거래대금' not in df.columns:
            print(f"⚠️ {market} 필수 컬럼 누락: {df.columns}")
            return pd.DataFrame()

        # 데이터 형변환
        df['거래대금'] = pd.to_numeric(df['거래대금'], errors='coerce').fillna(0)
        df['시가총액'] = pd.to_numeric(df.get('시가총액', 0), errors='coerce').fillna(0)
        
        return df[['티커', '종목명', '거래대금', '시가총액']]
        
    except Exception as e:
        print(f"❌ {market} 데이터 수집 실패: {e}")
        return pd.DataFrame()

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

    print("🚀 리포트 생성 시작...")
    
    # 1. 오늘(최신) 데이터 가져오기 (T일)
    # fdr.StockListing은 '현재 시점'의 최신 데이터를 가져옵니다.
    # 장 마감 후라면 '오늘 종가' 기준입니다.
    df_kospi = get_market_data_fdr('KOSPI')
    df_kosdaq = get_market_data_fdr('KOSDAQ')
    
    current_df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    
    if current_df.empty:
        print("❌ 전체 시장 데이터를 가져오지 못했습니다.")
        return None

    print(f"✅ 총 {len(current_df)}개 종목 데이터 확보.")

    # 2. 거래대금 상위 100개만 추리기 (속도 최적화)
    # 모든 종목의 과거 데이터를 다 조회하면 너무 느리므로, 
    # 오늘 거래대금이 좀 터진 애들만 대상으로 '어제보다 5배 늘었나?'를 검사합니다.
    # (오늘 거래대금이 0원이거나 작으면, 5배 늘어봤자 의미 없으므로)
    
    # 최소 거래대금 10억 이상인 종목 중 상위 100개
    candidates = current_df[current_df['거래대금'] >= 10_0000_0000].copy()
    candidates = candidates.sort_values(by='거래대금', ascending=False).head(100)
    
    print(f"🔍 거래대금 상위 {len(candidates)}개 종목에 대해 급증 여부 정밀 분석 시작...")
    
    results = []
    
    # 3. 과거 데이터와 비교 (T-1, T-2)
    # 후보 종목들만 개별적으로 과거 데이터를 조회합니다.
    now = dt.datetime.now(KST)
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - dt.timedelta(days=10)).strftime("%Y-%m-%d") # 넉넉하게
    
    for idx, row in candidates.iterrows():
        ticker = row['티커']
        name = row['종목명']
        curr_amt = row['거래대금'] # 오늘 대금
        
        try:
            # 해당 종목의 일별 시세 조회 (Volume, Close 등)
            # fdr은 거래대금(Amount) 컬럼을 주지 않는 경우가 많아, 종가*거래량으로 추산해야 할 수도 있음
            # 하지만 비교를 위해 추세만 보면 됨.
            
            # fdr.DataReader로 개별 종목 조회
            df_hist = fdr.DataReader(ticker, start=start_date, end=end_date)
            
            if len(df_hist) < 3:
                continue
                
            # 가장 최근 거래일 2개 가져오기
            # df_hist.iloc[-1]은 오늘(T), -2는 어제(T-1), -3은 그제(T-2)
            # 주의: 장 중이라면 -1이 오늘. 장 마감 후 데이터 업데이트가 늦으면 -1이 어제일 수도 있음.
            # 날짜를 확인해야 함.
            
            last_date = df_hist.index[-1].date()
            today_date = now.date()
            
            # 날짜 매칭 로직
            # Case A: 데이터 최신일이 오늘(2026-01-10) -> 장 중 or 장 마감 직후
            # 우리는 '전일(9일)' vs '전전일(8일)'을 비교하고 싶음? 
            # 아니면 '오늘(10일)' vs '어제(9일)'? -> 질문은 "전일 거래대금 급증"이므로
            # 보통 '오늘 마감된 장(T)' vs '어제(T-1)'을 비교함.
            
            # 여기서는 무조건 "가장 최근 데이터(T)"와 "그 직전 데이터(T-1)"을 비교합니다.
            
            vol_t = df_hist['Volume'].iloc[-1]   # 오늘 거래량
            close_t = df_hist['Close'].iloc[-1]  # 오늘 종가
            amt_t = vol_t * close_t # 대략적인 거래대금 (정확하진 않으나 비율 계산용으로 충분)
            
            vol_prev = df_hist['Volume'].iloc[-2] # 전일 거래량
            close_prev = df_hist['Close'].iloc[-2]
            amt_prev = vol_prev * close_prev
            
            if amt_prev == 0:
                continue
                
            ratio = amt_t / amt_prev
            
            # [조건] 5배 이상 급증
            if ratio >= 5.0:
                # StockListing에서 가져온 정확한 거래대금(row['거래대금'])을 사용
                results.append({
                    '티커': ticker,
                    '종목명': name,
                    '거래대금': row['거래대금'],
                    '시가총액': row['시가총액'],
                    '배수': ratio,
                    '기준일': last_date,
                    '대조일': df_hist.index[-2].date()
                })
                
        except Exception:
            continue

    # 결과 데이터프레임 생성
    if not results:
        print("📊 조건(5배 급증)을 만족하는 종목이 없습니다.")
        # 빈 결과라도 헤더는 보내기 위해
        target = pd.DataFrame()
        ref_date = now.date() # 임시
        ref_prev = now.date()
    else:
        target = pd.DataFrame(results)
        # 기준일/대조일은 첫 번째 결과물 기준으로 표기
        ref_date = target.iloc[0]['기준일']
        ref_prev = target.iloc[0]['대조일']
        
        # 정렬
        target = target.sort_values(by='거래대금', ascending=False).head(30)
        print(f"📊 최종 선별: {len(target)}개 종목")

    # 메시지 작성
    amts = []
    if not target.empty:
        for val in target["거래대금"].tolist():
            amts.append(f"{val/100000000:,.1f}")
        
        names = target["종목명"].tolist()
        tickers = target["티커"].tolist()
    else:
        names, tickers = [], []

    header = (
        f"[SK증권]\n"
        f"안녕하십니까 \n"
        f"sk 김수민입니다\n"
        f"전일거래대금 급증 종목 공유드립니다!\n"
        f"[기준일: {ref_date} vs {ref_prev}]\n"
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
    print(f"🚀 스크립트 실행 (현재시간: {dt.datetime.now(KST)})")
    
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
        

