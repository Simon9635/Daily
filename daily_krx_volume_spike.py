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
def get_volume_by_market(datestr: str, market: str) -> pd.DataFrame:
    """해당일/시장 티커별 거래량"""
    df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["티커", "거래량", "시장"])
    df = df.reset_index()
    vol_col = "거래량" if "거래량" in df.columns else next((c for c in df.columns if "량" in c), None)
    if not vol_col:
        return pd.DataFrame(columns=["티커", "거래량", "시장"])
    out = df[["티커", vol_col]].copy()
    out.rename(columns={vol_col: "거래량"}, inplace=True)
    out["거래량"] = pd.to_numeric(out["거래량"], errors="coerce")
    out.dropna(subset=["거래량"], inplace=True)
    out["거래량"] = out["거래량"].astype("int64")
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
        # CJK 폭 고려(W/F=2)
        w = 0
        for ch in s:
            w += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        return w

    def ljust_display(s: str, width: int) -> str:
        pad = max(0, width - disp_width(s))
        return s + (" " * pad)

    now = dt.datetime.now(KST)

    # 평일만 / 비교일 결정
    d1_date, d0_date = pick_compare_days(now)
    if d1_date is None:
        return None  # 주말 스킵

    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # 데이터 수집(KOSPI+KOSDAQ)
    vols_d1, vols_d0, caps_d1 = [], [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        vols_d1.append(get_volume_by_market(d1_str, mkt))
        vols_d0.append(get_volume_by_market(d0_str, mkt))
        caps_d1.append(get_mcap_by_market(d1_str, mkt))  # 시총은 전일 기준

    vol1 = pd.concat(vols_d1, ignore_index=True) if vols_d1 else pd.DataFrame(columns=["티커","거래량","시장"])
    vol0 = pd.concat(vols_d0, ignore_index=True) if vols_d0 else pd.DataFrame(columns=["티커","거래량","시장"])
    mcap = pd.concat(caps_d1, ignore_index=True) if caps_d1 else pd.DataFrame(columns=["티커","시가총액"])

    # 병합/필터(≥5배)
    merged = pd.merge(vol1, vol0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    for col in ["거래량_전일", "거래량_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래량_전일", "거래량_전전일"])
    merged = merged[merged["거래량_전전일"] > 0]
    merged["배수"] = (merged["거래량_전일"] / merged["거래량_전전일"]).round(2)
    result = merged[merged["배수"] >= 5].copy()

    # 시총 정렬 → 상위 30
    result = pd.merge(result, mcap, on="티커", how="left")
    result["시가총액"] = pd.to_numeric(result["시가총액"], errors="coerce").fillna(0)
    result.sort_values(by=["시가총액", "거래량_전일"], ascending=[False, False], inplace=True)
    result = result.head(30).reset_index(drop=True)

    # 종목명 매핑
    name_map = {}
    for t in result["티커"].tolist():
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # ===== 메시지 구성 =====
    header = (
        f"<b>[거래량 급증(≥5배) – 시총 상위 30개]</b>\n"
        f"기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}\n"
        f"(월=금↔목, 화=월↔금; 주말 미전송)\n"
    )
    if len(result) == 0:
        return header + "\n해당 없음."

    # 표시폭 계산
    names = [str(x or "") for x in result["종목명"].tolist()]
    vols  = [f"{int(v):,}" for v in result["거래량_전일"].tolist()]

    num_field_width = 3            # "1)" 포함 3칸
    lead_spaces = " " * (num_field_width + 1)  # 번호 뒤 공백까지
    name_width = max(2, max(disp_width(s) for s in names))
    gap_between = 2                # 종목명과 거래량 사이 공백

    # ----- 라벨 라인(앵커 확정) -----
    label_name = "종목명"
    label_vol  = "전일거래량"

    # 왼쪽에 번호영역 비우고 '종목명' 배치
    label_line = lead_spaces + label_name
    # 현재 라벨라인 표시폭
    cur_w = disp_width(label_line)

    # 거래량 라벨을 '우측 끝'이 아니라, 우리가 정한 '볼륨 열의 끝' 위치에 놓고,
    # 그 '끝 위치'를 이후 숫자들의 우측 정렬 기준(앵커)로 사용
    # 앵커 계산: 번호영역 + name_width + gap + label_vol 폭
    vol_anchor = disp_width(lead_spaces) + name_width + gap_between + disp_width(label_vol)
    # label_line에 필요한 공백 삽입 후 라벨 배치
    spaces_needed = max(0, vol_anchor - cur_w - disp_width(label_vol))
    label_line = label_line + (" " * spaces_needed) + label_vol

    lines = [f"<code>{html.escape(label_line)}</code>"]

    # ----- 데이터 라인(숫자를 라벨의 '끝'에 정렬) -----
    for i, (nm, vv) in enumerate(zip(names, vols), start=1):
        num = f"{i})"
        left = f"{num:<{num_field_width}} "
        nm_padded = ljust_display(nm, name_width)
        base = left + nm_padded + (" " * gap_between)
        # 남은 패딩 = (라벨 끝 앵커) - (현재 왼쪽부분 폭) - (숫자 길이)
        rem = max(0, vol_anchor - disp_width(base) - len(vv))
        line_plain = base + (" " * rem) + vv  # 쉼표 없이 정렬
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

