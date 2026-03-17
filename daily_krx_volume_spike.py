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
        f"<b><u>전일거래대금 급증 종목 공유드립니다!</u></b>\n"
        f"[기준일: {ref_date} vs {ref_prev}]\n"
        f"\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    W_NUM = 1; W_NAME = 11; W_AMT = 11; GAP = "   "

    h_line = (
        f"{'   '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align(' 거래대금(억)', W_AMT)}{GAP}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 26)

    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME)
        amt_s = center_align(av, W_AMT)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}"
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
