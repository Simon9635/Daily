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
import FinanceDataReader as fdr

# --- Telegram ENV ---
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

from pykrx import stock
import pandas as pd

KST = dt.timezone(dt.timedelta(hours=9))
TG_MAX = 4096

# ---------- Telegram ----------
def tg_send(text: str):
    """HTML 파싱 이슈/길이 초과를 방어하며 전송"""
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
                js = json.loads(resp.read().decode("utf-8"))
                if not js.get("ok"):
                    raise RuntimeError(f"Telegram API error: {js}")
        except Exception as e:
            try:
                desc = e.read().decode("utf-8", "ignore") if hasattr(e, "read") else str(e)
            except Exception:
                desc = str(e)
            raise RuntimeError(f"Telegram sendMessage failed: {desc}") from e

    if len(text) <= TG_MAX:
        try:
            _post(text, parse_html=True)
        except RuntimeError:
            _post(text, parse_html=False)
        return

    i = 0
    while i < len(text):
        chunk = text[i:i+TG_MAX]
        try:
            _post(chunk, parse_html=True)
        except RuntimeError:
            _post(chunk, parse_html=False)
        i += TG_MAX

# ---------- Date picking ----------
def _prev_weekday(d: dt.date) -> dt.date:
    d -= dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d

def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    [수정됨] 휴장일(1월 1일, 명절 등)을 자동으로 건너뛰고
    실제 '오늘(장 열린 날)'과 '직전 거래일'을 찾아냅니다.
    """
    try:
        # 1. 오늘 날짜 구하기
        target_date = now_kst.date()
        
        # 2. 넉넉하게 최근 2주(14일)치 주가 데이터를 조회 (삼성전자 코드 005930 이용)
        # (개별 종목 데이터를 쓰는 이유는 가장 빠르고 정확하게 거래일을 알 수 있기 때문입니다)
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        # pykrx를 이용해 삼성전자 일별 시세 조회
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        # 데이터가 너무 적으면(최소 2일 필요) 실패 처리
        if df.empty or len(df) < 2:
            return None, None
            
        # 3. 거래일 리스트 확보 (index가 날짜임)
        valid_dates = df.index.tolist() # [..., 12월30일, 1월2일, 1월5일]
        
        # 4. '오늘'이 장이 열린 날인지 확인
        # DB에서 가져온 가장 최근 날짜(last_biz_day)가 오늘(target_date)과 같은지 체크
        last_biz_day = valid_dates[-1].date()
        
        if last_biz_day != target_date:
            # 오늘은 주말이거나 공휴일이라 장이 안 열렸음 (또는 장 마감 전이라 데이터 없음)
            return None, None
            
        # 5. 오늘(d1)과 바로 직전 거래일(d0) 리턴
        d1 = valid_dates[-1].date() # 오늘
        d0 = valid_dates[-2].date() # 직전 거래일 (어제가 휴일이면 그 전날이 됨)
        
        return d1, d0
        
    except Exception:
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- Data pulls ----------
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

# ---------- [차트 분석 함수] ----------
def get_chart_status(ticker: str) -> str:
    """FinanceDataReader 사용 (안정성 강화)"""
    try:
        now = dt.datetime.now(KST)
        today = now.strftime("%Y-%m-%d")
        start_day = (now - dt.timedelta(days=120)).strftime("%Y-%m-%d")
        
        df = fdr.DataReader(ticker, start=start_day, end=today)
        
        if df.empty or len(df) < 60:
            return "-"

        close = df['Close']
        current_price = close.iloc[-1]
        
        # 1. 신고가
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

# ---------- Build & send ----------
def build_report():
    import unicodedata, html

    # [1] 글자 폭 계산
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
    if d1_date is None:
        return None 

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # 데이터 수집
    vals_d1, vals_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vals_d1.append(get_trading_value_by_market(d1_str, mkt))
        vals_d0.append(get_trading_value_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))

    val1 = pd.concat(vals_d1, ignore_index=True) if vals_d1 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    val0 = pd.concat(vals_d0, ignore_index=True) if vals_d0 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커", "시가총액"])

    # 필터링
    merged = pd.merge(val1, val0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래대금_전일", "거래대금_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래대금_전일", "거래대금_전전일"])
    merged = merged[merged["거래대금_전전일"] > 0]
    merged["배수"] = (merged["거래대금_전일"] / merged["거래대금_전전일"]).round(2)

    result = merged[merged["배수"] >= 5].copy()

    # 정렬
    result = pd.merge(result, mcap, on="티커", how="left")
    result["시가총액"] = pd.to_numeric(result["시가총액"], errors="coerce").fillna(0)
    result.sort_values(by=["시가총액", "거래대금_전일"], ascending=[False, False], inplace=True)
    result = result.head(30).reset_index(drop=True)

    # 종목명 매핑
    name_map = {}
    for t in result["티커"].tolist():
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # 거래대금 포맷팅
    amts = []
    for v in result["거래대금_전일"].tolist():
        v_eok = int(v) / 100_000_000
        amts.append(f"{v_eok:,.1f}")

    names = [str(x or "") for x in result["종목명"].tolist()]
    tickers = result["티커"].tolist()

    # 메시지 헤더
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 sk 김수민입니다\n"
        f"<b>전일거래대금 급증 종목 공유드립니다!</b>\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    # ===== [설정] 테이블 칼럼 너비 (1칸씩 증가됨) =====
    W_RANK = 3
    W_NAME = 13    # 기존 12 -> 13
    W_AMT  = 11    # 기존 10 -> 11
    W_CHART= 11    # 기존 10 -> 11
    
    GAP    = "    "  # 4칸 (기존 유지)
    GAPS   = " "
    GAPSS  = "  "

    # 1. 헤더 생성
    h_rank = " " * W_RANK
    h_name = center_align("종목", W_NAME)
    h_amt  = center_align("거래대금(억)", W_AMT)
    h_chart= center_align("차트", W_CHART)
    
    # 헤더 라인 조립
    header_line = f"{h_rank}{GAPSS}{h_name}{GAP}{GAPS}{h_amt}{GAP}{GAPS}{h_chart}"
    lines = [f"<code>{html.escape(header_line)}</code>"]
    lines.append("-" * 45)

    # 2. 데이터 라인 생성
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), start=1):
        rank_str = f"{str(i)+')':<{W_RANK}}"
        
        # 종목명 5글자 컷
        nm_trunc = nm[:5] 
        name_str = center_align(nm_trunc, W_NAME)
        amt_str = center_align(av, W_AMT)
        
        # 차트 분석
        status = get_chart_status(t_code)
        chart_str = center_align(status, W_CHART)
        
        row_str = f"{rank_str}{GAPS}{name_str}{GAP}{amt_str}{GAP}{chart_str}"
        lines.append(f"<code>{html.escape(row_str)}</code>")

    return header + "\n" + "\n".join(lines)
    
if __name__ == "__main__":
    try:
        msg = build_report()
        if msg is not None:
            tg_send(msg)
    except Exception as e:
        try:
            tg_send(f"⚠️ 자동화 에러: {e}")
        except Exception:
            pass
        sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)
