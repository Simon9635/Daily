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

    # [1] 글자 폭 계산 함수 (한글=2칸, 영어/숫자=1칸)
    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    # [2] 가운데 정렬 함수 (핵심)
    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width:
            return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    if d1_date is None:
        return None  # 주말 스킵

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # --- 데이터 수집 ---
    vals_d1, vals_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vals_d1.append(get_trading_value_by_market(d1_str, mkt))
        vals_d0.append(get_trading_value_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))

    val1 = pd.concat(vals_d1, ignore_index=True) if vals_d1 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    val0 = pd.concat(vals_d0, ignore_index=True) if vals_d0 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커", "시가총액"])

    # --- 필터링 (5배 급증) ---
    merged = pd.merge(val1, val0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래대금_전일", "거래대금_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래대금_전일", "거래대금_전전일"])
    merged = merged[merged["거래대금_전전일"] > 0]
    merged["배수"] = (merged["거래대금_전일"] / merged["거래대금_전전일"]).round(2)

    result = merged[merged["배수"] >= 5].copy()

    # --- 정렬 및 상위 30개 ---
    result = pd.merge(result, mcap, on="티커", how="left")
    result["시가총액"] = pd.to_numeric(result["시가총액"], errors="coerce").fillna(0)
    result.sort_values(by=["시가총액", "거래대금_전일"], ascending=[False, False], inplace=True)
    result = result.head(30).reset_index(drop=True)

    # --- 종목명 매핑 ---
    name_map = {}
    for t in result["티커"].tolist():
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # ---- 거래대금 변환 ----
    amts = []
    for v in result["거래대금_전일"].tolist():
        v_eok = int(v) / 100_000_000
        amts.append(f"{v_eok:,.1f}")

    names = [str(x or "") for x in result["종목명"].tolist()]
    tickers = result["티커"].tolist()

    # ===== 메시지 헤더 =====
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 sk 김수민입니다\n"
        f"<b>전일거래대금 급증 종목 공유드립니다!</b>\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    # ===== [설정] 테이블 칼럼 너비 및 간격 (가운데 정렬용) =====
    # 전체 모양: [순위(3)] [공백] [종목(10)] [공백] [대금(9)] [공백] [차트(10)]
    W_RANK = 3    # "1) "
    W_NAME = 12   # 종목명 (넉넉하게 6글자 폭)
    W_AMT  = 10   # 거래대금 (1,234.5)
    W_CHART= 10   # 차트 (이모지 포함)
    GAP    = " "  # 투명한 줄(Separation) 역할 (공백 1칸)

    # 1. 헤더 생성 (가운데 정렬 적용)
    h_rank = " " * W_RANK
    h_name = center_align("종목", W_NAME)
    h_amt  = center_align("대금", W_AMT)
    h_chart= center_align("차트", W_CHART)
    
    # 헤더 라인 조립
    header_line = f"{h_rank}{GAP}{h_name}{GAP}{h_amt}{GAP}{h_chart}"
    lines = [f"<code>{html.escape(header_line)}</code>"]
    lines.append("-" * 40) # 구분선

    # 2. 데이터 라인 생성
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), start=1):
        # (1) 순위: 왼쪽 정렬 (가독성 위해)
        rank_str = f"{str(i)+')':<{W_RANK}}"

        # (2) 종목명: 5글자로 자르고 가운데 정렬
        nm_trunc = nm[:5] 
        name_str = center_align(nm_trunc, W_NAME)

        # (3) 거래대금: 가운데 정렬
        amt_str = center_align(av, W_AMT)

        # (4) 차트위치: 가운데 정렬
        status = get_chart_status(t_code) # [주의] 위에서 수정한 함수(인자 1개)여야 함
        chart_str = center_align(status, W_CHART)
        
        # 라인 조립 (Code Block으로 감싸서 정렬 유지)
        row_str = f"{rank_str}{GAP}{name_str}{GAP}{amt_str}{GAP}{chart_str}"
        lines.append(f"<code>{html.escape(row_str)}</code>")

    return header + "\n" + "\n".join(lines)

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


