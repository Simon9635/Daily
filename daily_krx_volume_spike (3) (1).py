#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata  # <--- 이 줄을 import 모여있는 곳에 추가해주세요

# --- [뉴스 크롤링 라이브러리] ---
import requests
from bs4 import BeautifulSoup
import re

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

# ---------- Date picking (평일만 / 월=금↔목, 화=월↔금) ----------
def _prev_weekday(d: dt.date) -> dt.date:
    d -= dt.timedelta(days=1)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= dt.timedelta(days=1)
    return d

def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    평일만 전송:
      - Mon: (Fri, Thu)
      - Tue: (Mon, Fri)
      - Wed: (Tue, Mon)
      - Thu: (Wed, Tue)
      - Fri: (Thu, Wed)
    주말이면 (None, None)
    """
    wd = now_kst.weekday()  # Mon=0 ... Sun=6
    if wd >= 5:
        return None, None
    today = now_kst.date()
    if wd == 0:  # Mon
        d1 = today - dt.timedelta(days=3)  # Fri
        d0 = today - dt.timedelta(days=4)  # Thu
    elif wd == 1:  # Tue
        d1 = today - dt.timedelta(days=1)  # Mon
        d0 = today - dt.timedelta(days=4)  # Fri
    else:  # Wed~Fri
        d1 = _prev_weekday(today)
        d0 = _prev_weekday(d1)
    return d1, d0

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- Data pulls ----------
def get_trading_value_by_market(datestr: str, market: str) -> pd.DataFrame:
    """
    해당일/시장 티커별 '거래대금'을 반환하는 테이블.
    pykrx get_market_ohlcv_by_ticker에서 '거래대금' 컬럼 사용.
    """
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

def get_mcap_by_market(datestr: str, market: str) -> pd.DataFrame:
    """해당일/시장 티커별 시가총액"""
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

def safe_int(n):
    try:
        return int(n)
    except Exception:
        return 0

# [수정] 차트 위치 분석 함수 (입력값에서 current_price 제거함)
def get_chart_status(ticker: str) -> str:
    """
    최근 60일 데이터를 분석하여 차트 위치(신고가, 역배열 등) 반환
    (내부에서 현재가를 직접 조회하도록 수정됨)
    """
    try:
        # 오늘 기준 최근 120일 데이터 조회
        now = dt.datetime.now(KST)
        today = now.strftime("%Y%m%d")
        start_day = (now - dt.timedelta(days=120)).strftime("%Y%m%d")
        
        # 일별 시세 조회
        df = stock.get_market_ohlcv_by_date(start_day, today, ticker)
        
        if df.empty or len(df) < 60:
            return "-" # 데이터 부족

        close = df['종가']
        current_price = close.iloc[-1] # [중요] 현재가를 여기서 직접 계산
        
        # 1. 52주 신고가 근처 (최근 60일 최고가 기준 판단)
        recent_high = close.max()
        if current_price >= recent_high * 0.98:
            return "🔥신고가"

        # 이동평균선 계산
        ma5 = close.rolling(window=5).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        ma60 = close.rolling(window=60).mean().iloc[-1]

        # 2. 역배열 (60 > 20 > 5) - 바닥권 가능성
        if ma60 > ma20 > ma5:
            return "📉역배열"

        # 3. 박스권 상단 (최근 저점 대비 80% 이상 위치)
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

    # ---- 한글 2칸 폭 고려한 표시폭 계산 ----
    def disp_width(s: str) -> int:
        w = 0
        for ch in s:
            # W/F = wide/fullwidth → 2칸, 나머지 1칸
            w += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        return w

    def ljust_display(s: str, width_units: int) -> str:
        """표시폭(width_units 기준) 만큼 좌측 정렬 + 공백 패딩."""
        cur = disp_width(s)
        pad = max(0, width_units - cur)
        return s + (" " * pad)

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    if d1_date is None:
        return None  # 주말은 스킵

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # --- 거래대금 / 시총 수집 (KOSPI + KOSDAQ) ---
    vals_d1, vals_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vals_d1.append(get_trading_value_by_market(d1_str, mkt))
        vals_d0.append(get_trading_value_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))

    val1 = pd.concat(vals_d1, ignore_index=True) if vals_d1 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    val0 = pd.concat(vals_d0, ignore_index=True) if vals_d0 else pd.DataFrame(columns=["티커", "거래대금", "시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커", "시가총액"])

    # --- 전일/전전일 거래대금 5배 필터 ---
    merged = pd.merge(val1, val0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래대금_전일", "거래대금_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래대금_전일", "거래대금_전전일"])
    merged = merged[merged["거래대금_전전일"] > 0]
    merged["배수"] = (merged["거래대금_전일"] / merged["거래대금_전전일"]).round(2)

    result = merged[merged["배수"] >= 5].copy()

    # --- 시총 기준 정렬 → 상위 30개 ---
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

    # ---- 거래대금 억 단위(소수 1자리)로 변환 ----
    amts = []
    for v in result["거래대금_전일"].tolist():
        v_krw = int(v)
        v_eok = v_krw / 100_000_000  # 원 → 억
        amts.append(f"{v_eok:,.1f}")

    names = [str(x or "") for x in result["종목명"].tolist()]

    # ===== 메시지 헤더 =====
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 sk 김수민입니다\n"
        f"<b>전일거래대금 급증 종목 공유드립니다!</b>\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    # ===== 고정 포맷 설정 =====
    num_field_width   = 3          # "1)" 영역
    NAME_WIDTH_UNITS  = 16         # ✅ 화면 표시폭 기준 16칸(한글 8글자까지 커버)
    gap_na            = 3          # 종목명과 전일거래대금 사이 공백
    amt_label         = "거래대금(억)"

    def format_name(s: str) -> str:
        # 종목명은 최대 5글자까지만 사용 (그 이상은 잘라냄)
        s_trunc = s[:5]
        # 표시폭 기준 16칸이 되도록 공백 패딩
        return ljust_display(s_trunc, NAME_WIDTH_UNITS)

    # === [수정됨] 티커 리스트 가져오기 ===
    tickers = result["티커"].tolist() 

    # ─ 라벨 라인: 종목명 / 대금 / 재료 ─
    lead = " " * (num_field_width + 1)
    name_label_cell = format_name("종목명")

    # 차트 칼럼 목표 너비: 10칸 (이모지+글자 고려)
    CHART_WIDTH = 10 
    header_chart = ljust_display("차트", CHART_WIDTH).replace("차트", "   차트")

    # 헤더에 '재료' 추가
    label_line_plain = f"{lead}{name_label_cell}{' ' * gap_na}{amt_label} {'차트'}"
    lines = [f"<code>{html.escape(label_line_plain)}</code>"]

# ─ 데이터 라인 수정 (for문 교체) ─
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), start=1):
        num_cell  = f"{str(i) + ')':<{num_field_width}}"
        name_cell = format_name(nm)
        
        # 1. 차트 상태 분석 (함수 호출)
        status = get_chart_status(t_code)
        
        # 2. [오른쪽 정렬] 차트 상태 패딩 계산
        # (목표 너비 - 실제 글자 너비) 만큼 왼쪽에 공백을 채움
        pad_len = max(0, CHART_WIDTH - disp_width(status))
        padding = " " * pad_len
        
        # 3. 한 줄 완성 (전부 Code Block 안에 넣어서 정렬 유지)
        line_fixed = f"{num_cell} {name_cell}{' ' * gap_na}{av}   {padding}{status}"
        
        lines.append(f"<code>{html.escape(line_fixed)}</code>")

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

