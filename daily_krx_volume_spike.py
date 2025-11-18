#!/usr/bin/env python3
import os
import sys
import json
import html
import datetime as dt
from urllib import request, parse

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
    val_col = "거래대금" if "거래대금" in df.columns else next((c for c in df.columns if "대금" in c), None)
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

# ---------- Build & send ----------
def build_report():
    import unicodedata

    def disp_width(s: str) -> int:
        w = 0
        for ch in s:
            w += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        return w

    def ljust_display(s: str, width: int) -> str:
        pad = max(0, width - disp_width(s))
        return s + (" " * pad)

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    if d1_date is None:
        return None

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # --- 데이터 수집: 거래대금/시총 (KOSPI+KOSDAQ) ---
    vals_d1, vals_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vals_d1.append(get_trading_value_by_market(d1_str, mkt))
        vals_d0.append(get_trading_value_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))  # 정렬용 시총은 '전일' 기준

    val1 = pd.concat(vals_d1, ignore_index=True) if vals_d1 else pd.DataFrame(columns=["티커","거래대금","시장"])
    val0 = pd.concat(vals_d0, ignore_index=True) if vals_d0 else pd.DataFrame(columns=["티커","거래대금","시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커","시가총액"])

    # --- 병합 및 필터: 전일/전전일 거래대금 5배 ---
    merged = pd.merge(val1, val0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래대금_전일", "거래대금_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래대금_전일", "거래대금_전전일"])
    merged = merged[merged["거래대금_전전일"] > 0]
    merged["배수"] = (merged["거래대금_전일"] / merged["거래대금_전전일"]).round(2)
    result = merged[merged["배수"] >= 5].copy()

    # --- 시총 정렬 → 상위 30 ---
    result = pd.merge(result, mcap, on="티커", how="left")
    result["시가총액"] = pd.to_numeric(result["시가총액"], errors="coerce").fillna(0)
    result.sort_values(by=["시가총액", "거래대금_전일"], ascending=[False, False], inplace=True)
    result = result.head(30).reset_index(drop=True)

    # --- 종목명 매핑 ---
    name_map, tickers = {}, result["티커"].tolist()
    for t in tickers:
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # ===== 메시지 구성 (라벨을 각 열 위에 정렬, 쉼표 없음, 끝자리 맞춤) =====
    header = (
        f"<b>[거래대금 급증(≥5배) – 시총 상위 30개]</b>\n"
        f"기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}\n"
        f"(월=금↔목, 화=월↔금; 주말 미전송)\n"
    )
    if len(result) == 0:
        return header + "\n해당 없음."

    names = [str(x or "") for x in result["종목명"].tolist()]
    amts  = [f"{int(v):,}" for v in result["거래대금_전일"].tolist()]  # 원 단위, 콤마 표기

    num_field_width = 3  # "1)" 3칸
    name_width = max(2, max(disp_width(s) for s in names))
    amt_width  = max(4, max(len(s) for s in amts))
    gap_na, gap_an = 2, 2  # 이름-대금, 대금-비고 사이 공백

    # 전체 목표 폭 = 번호+공백 + name + gap_na + amt
    total_width = (num_field_width + 1) + name_width + gap_na + amt_width 

    # ─ 라벨 라인: '종목명'은 이름열 위, '전일거래대금'은 수치열의 '우측 끝' 위, '비고'는 비고열 시작 위 ─
    lead = " " * (num_field_width + 1)
    label_line = lead + "종목명"
    cur_w = disp_width(label_line)

    # 거래대금 라벨을 수치열의 '오른쪽 끝' 기준으로 정렬
    start_amt_left = (num_field_width + 1) + name_width + gap_na
    right_edge_amt = start_amt_left + amt_width
    pad_for_amt = max(1, right_edge_amt - disp_width("전일거래대금") - cur_w)
    label_line += " " * pad_for_amt + "전일거래대금"

    lines = [f"<code>{html.escape(label_line)}</code>"]

    # ─ 데이터 라인: 번호 + 종목명 좌정렬 + 거래대금 우정렬(끝자리 맞춤) + 비고 ─
    for i, (nm, av, nt) in enumerate(zip(names, amts), start=1):
        left = f"{(str(i)+')'):<{num_field_width}} " + ljust_display(nm, name_width) + (" " * gap_na)
        cur = disp_width(left)
        # 거래대금 우측 끝 맞추기
        target_amt_right = (num_field_width + 1) + name_width + gap_na + amt_width
        pad_left_amt = max(0, target_amt_right - len(av) - cur)
        line = left + (" " * pad_left_amt) + av
        
        lines.append(f"<code>{html.escape(line_plain)}</code>")

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

