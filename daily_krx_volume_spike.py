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

# ---------- [핵심 수정] 차트 분석 로직 개선 ----------
def get_chart_status(ticker: str) -> str:
    try:
        now = dt.datetime.now(KST)
        today = now.strftime("%Y-%m-%d")
        # [변경 1] 기간을 넉넉하게 180일 전부터 가져옴 (이평선 120일 계산 위해)
        start_day = (now - dt.timedelta(days=180)).strftime("%Y-%m-%d")
        
        df = fdr.DataReader(ticker, start=start_day, end=today)
        # 데이터가 너무 적으면 분석 불가
        if df.empty or len(df) < 120:
            return "-"

        close = df['Close']
        curr = close.iloc[-1]
        
        # [변경 2] 최근 120일(약 6개월) 기준으로 고가/저가 산출
        window = 120
        recent_high = close.tail(window).max()
        recent_low = close.tail(window).min()
        
        # 현재 위치 (0.0 = 최저가, 1.0 = 최고가)
        if recent_high == recent_low:
            position = 0.5
        else:
            position = (curr - recent_low) / (recent_high - recent_low)

        # 이평선 계산
        ma5 = close.rolling(5).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]
        ma120 = close.rolling(120).mean().iloc[-1] # 120일선 추가

        # --- [변경 3] 판단 우선순위 재조정 ---
        
        # 1. 바닥권인지 먼저 확인 (급등했어도 전체 위치가 낮으면 바닥권)
        # (하위 25% 이내)
        if position <= 0.25:
            return "💧바닥권"

        # 2. 역배열 확인 (장기 이평선이 위에 있는지)
        # 120일선 > 60일선 > 20일선 구조면 역배열로 간주
        if ma120 > ma60 > ma20:
            return "📉역배열"

        # 3. 정배열 확인 (단기 > 중기 > 장기)
        if ma5 > ma20 > ma60 > ma120:
            return "📈정배열"

        # 4. 신고가 확인 (상위 2% 이내)
        if curr >= recent_high * 0.98:
            return "🔥신고가"

        # 5. 박스권 상단 (상위 20% 이내)
        if position >= 0.8:
            return "📦박스상단"

        return "⚖️중립"
    except:
        return "-"

# ---------- FDR StockListing 데이터 수집 ----------
def get_market_data_fdr(market: str):
    try:
        print(f"📥 {market} 데이터 다운로드 중 (StockListing)...")
        df = fdr.StockListing(market)
        
        if df is None or df.empty:
            return pd.DataFrame()
            
        cols = df.columns
        col_map = {
            'Code': '티커', 'Symbol': '티커',
            'Name': '종목명',
            'Close': '종가', 
            'Amount': '거래대금', 
            'Marcap': '시가총액'
        }
        df = df.rename(columns=col_map)
        
        if '티커' not in df.columns or '거래대금' not in df.columns:
            print(f"⚠️ {market} 필수 컬럼 누락: {df.columns}")
            return pd.DataFrame()

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
    
    df_kospi = get_market_data_fdr('KOSPI')
    df_kosdaq = get_market_data_fdr('KOSDAQ')
    
    current_df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    
    if current_df.empty:
        print("❌ 전체 시장 데이터를 가져오지 못했습니다.")
        return None

    print(f"✅ 총 {len(current_df)}개 종목 데이터 확보.")

    # 거래대금 10억 이상 상위 100개 추리기
    candidates = current_df[current_df['거래대금'] >= 10_0000_0000].copy()
    candidates = candidates.sort_values(by='거래대금', ascending=False).head(100)
    
    print(f"🔍 거래대금 상위 {len(candidates)}개 종목 분석 중...")
    
    results = []
    now = dt.datetime.now(KST)
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - dt.timedelta(days=10)).strftime("%Y-%m-%d")
    
    for idx, row in candidates.iterrows():
        ticker = row['티커']
        name = row['종목명']
        
        try:
            # 개별 종목 차트 데이터 조회
            df_hist = fdr.DataReader(ticker, start=start_date, end=end_date)
            
            if len(df_hist) < 3:
                continue
                
            last_date = df_hist.index[-1].date()
            
            vol_t = df_hist['Volume'].iloc[-1]
            close_t = df_hist['Close'].iloc[-1]
            amt_t = vol_t * close_t 
            
            vol_prev = df_hist['Volume'].iloc[-2]
            close_prev = df_hist['Close'].iloc[-2]
            amt_prev = vol_prev * close_prev
            
            if amt_prev == 0:
                continue
                
            ratio = amt_t / amt_prev
            
            # [조건] 5배 이상 급증
            if ratio >= 5.0:
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

    if not results:
        print("📊 조건 만족 종목 없음.")
        target = pd.DataFrame()
        ref_date = now.date()
        ref_prev = now.date()
    else:
        target = pd.DataFrame(results)
        ref_date = target.iloc[0]['기준일']
        ref_prev = target.iloc[0]['대조일']
        
        # [정렬 기준] 시가총액 순 (큰 종목부터)
        target = target.sort_values(by='시가총액', ascending=False).head(30)
        print(f"📊 최종 선별: {len(target)}개 종목 (시가총액 순)")

    # 포맷팅
    amts = []
    if not target.empty:
        for val in target["거래대금"].tolist():
            amts.append(f"{val/100000000:,.0f}")
        
        names = target["종목명"].tolist()
        tickers = target["티커"].tolist()
    else:
        names, tickers = [], []

    header = (
        f"[SK증권]\n"
        f"안녕하십니까,\n"
        f"<b><u>전일거래대금 급증 종목 공유드립니다!</u></b>\n"
        f"[기준일: {ref_date} vs {ref_prev}]\n"
        f"\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    W_NUM = 1; W_NAME = 11; W_AMT = 11; W_CHART = 9; GAP = "   "

    h_line = (
        f"{'   '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align(' 거래대금(억)', W_AMT)}{GAP}"
        f"{center_align(' 차트', W_CHART)}"
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
