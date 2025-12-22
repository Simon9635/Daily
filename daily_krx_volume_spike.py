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

# ---------- [업데이트 1] 뉴스 키워드 지능형 추출 ----------
def get_news_keyword(ticker: str) -> str:
    try:
        url = f"https://finance.naver.com/item/news_news.nhn?code={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': f'https://finance.naver.com/item/news.nhn?code={ticker}'
        }
        resp = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # iframe 내 뉴스 리스트 선택자 (tbody 제거)
        item = soup.select_one(".type5 .title a")
        if not item: return ""
        
        title = item.get_text().strip()
        
        # 1. 말머리 및 괄호 제거
        title = re.sub(r'\[.*?\]|\(.*?\)|\<.*?\>', '', title)
        
        # 2. 불필요한 단어(Stop words) 제거 리스트 업데이트
        stop_words = [
            "특징주", "급등", "강세", "상승", "하락", "급락", "주가", "관련주", "영향", "부각", 
            "소식", "체결", "::", "전일대비", "오전", "오후", "장중", "마감", "속보", "공시", 
            "발표", "분석", "전망", "실적", "최대", "개선", "회복", "우려", "기대", "감소", 
            "증가", "돌파", "경신", "유입", "확대", "축소", "약세", "보합", "출발", "상위", 
            "종목", "투자", "유치", "확보", "개발", "성공", "승인", "허가", "취득", "공급", 
            "계약", "협력", "제휴", "진출", "본격화", "개시", "시작"
        ]
        for w in stop_words:
            title = title.replace(w, " ")

        # 3. 핵심 단어만 남기기 (7자 제한)
        keywords = ""
        for word in title.split():
            if not any(c.isalnum() for c in word): continue # 특수문자만 있으면 패스
            
            # 합쳤을 때 7자 넘어가면 중단
            if len(keywords + word) > 7:
                if not keywords: keywords = word[:6] + "."
                break
            keywords += word + " "
            
        return keywords.strip()
    except Exception:
        return ""

# [추가] 글자 폭 계산 함수 (한글=2, 영어=1)
def disp_width(s):
    return sum(2 if unicodedata.east_asian_width(c) in 'WF' else 1 for c in s)

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
    # 헤더에 '재료' 추가
    label_line_plain = f"{lead}{name_label_cell}{' ' * gap_na}{amt_label} {'재료'}"
    lines = [f"<code>{html.escape(label_line_plain)}</code>"]

# ─ 데이터 라인 생성 (for문 전체 교체) ─
    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), start=1):
        num_cell  = f"{str(i) + ')':<{num_field_width}}"
        name_cell = format_name(nm)
        
        # 1. 키워드 가져오기
        kwd = get_news_keyword(t_code)
        
        # 2. [오른쪽 정렬 핵심] 왼쪽 패딩 계산
        # '재료' 칼럼의 목표 너비를 14칸(한글 7자)으로 잡음
        TARGET_WIDTH = 14
        
        if kwd:
            # (목표 너비 - 실제 글자 너비) 만큼 공백 생성
            space_len = max(0, TARGET_WIDTH - disp_width(kwd))
            padding = " " * space_len
            
            # [중요] Telegram에서는 <code> 안에 공백을 넣어야 간격이 유지됨
            # 기존 정보 + 패딩까지 회색 박스로 감싸고 -> 그 뒤에 링크를 붙임
            row_str = f"<code>{html.escape(num_cell)} {html.escape(name_cell)}{' ' * gap_na}{av}{padding}</code><a href='https://finance.naver.com/item/news_news.nhn?code={t_code}'>{html.escape(kwd)}</a>"
        else:
            # 뉴스가 없으면 그냥 회색 박스 닫기
            row_str = f"<code>{html.escape(num_cell)} {html.escape(name_cell)}{' ' * gap_na}{av}</code>"

        lines.append(row_str)

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

