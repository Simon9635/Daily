#!/usr/bin/env python3
import os
import sys
import json
import datetime as dt
from urllib import request, parse

# --- Telegram ENV ---
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

from pykrx import stock
import pandas as pd

KST = dt.timezone(dt.timedelta(hours=9))

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    body = parse.urlencode(data).encode("utf-8")
    req = request.Request(url, data=body, method="POST")
    with request.urlopen(req, timeout=30) as resp:
        js = json.loads(resp.read().decode("utf-8"))
        if not js.get("ok"):
            raise RuntimeError(f"Telegram API error: {js}")

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    평일만 전송:
      - 월요일: (금요일, 목요일)
      - 화~금: (전일, 전전일)
    주말이면 None 리턴 -> 전송 스킵
    * 공휴일은 별도 고려하지 않습니다(요청사항 충족 관점).
    """
    wd = now_kst.weekday()  # Mon=0 ... Sun=6
    if wd in (5, 6):  # Sat, Sun
        return None, None

    today = now_kst.date()
    if wd == 0:  # Monday
        d1 = today - dt.timedelta(days=3)  # Friday
        d0 = today - dt.timedelta(days=4)  # Thursday
    else:  # Tue~Fri
        d1 = today - dt.timedelta(days=1)  # yesterday
        d0 = today - dt.timedelta(days=2)  # day before yesterday
    return d1, d0

def get_volume_by_market(datestr: str, market: str) -> pd.DataFrame:
    """
    해당일/시장 티커별 거래량 데이터프레임 (숫자형 강제)
    """
    df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["티커", "거래량", "시장"])
    df = df.reset_index()  # index에 티커가 들어오는 pykrx 포맷 방지
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

def safe_int(n):
    try:
        return int(n)
    except Exception:
        return 0

def build_report():
    now = dt.datetime.now(KST)

    # 1) 주말 스킵 + 비교일 선택
    d1_date, d0_date = pick_compare_days(now)
    if d1_date is None:
        # 주말: 전송하지 않음 (조용히 종료)
        return None

    # 2) 비교일 문자열
    d1_str, d0_str = yyyymmdd(d1_date), yyyymmdd(d0_date)

    # 3) 두 시장 데이터 수집
    frames_d1, frames_d0 = [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        frames_d1.append(get_volume_by_market(d1_str, mkt))
        frames_d0.append(get_volume_by_market(d0_str, mkt))
    vol1 = pd.concat(frames_d1, ignore_index=True) if frames_d1 else pd.DataFrame(columns=["티커","거래량","시장"])
    vol0 = pd.concat(frames_d0, ignore_index=True) if frames_d0 else pd.DataFrame(columns=["티커","거래량","시장"])

    # 4) 병합 및 계산
    merged = pd.merge(vol1, vol0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    merged["시장"] = merged["시장_전일"] if "시장_전일" in merged.columns else merged.get("시장", "KRX")

    for col in ["거래량_전일", "거래량_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.dropna(subset=["거래량_전일", "거래량_전전일"])
    merged = merged[merged["거래량_전전일"] > 0]

    merged["배수"] = (merged["거래량_전일"] / merged["거래량_전전일"]).round(2)
    result = merged[merged["배수"] >= 5].copy()

    # 5) 종목명 매핑
    name_map = {}
    for t in result["티커"].tolist():
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    result.sort_values(by=["배수", "거래량_전일"], ascending=[False, False], inplace=True)

    # 6) 메시지 구성
    header = (
        f"<b>[KOSPI/KOSDAQ 거래량 급증 리스트]</b>\n"
        f"기준: {yyyy_mm_dd(d1_date)}(전일) vs {yyyy_mm_dd(d0_date)}(전전일)\n"
        f"조건: 전일 거래량 ≥ 전전일의 <b>5배</b>\n"
        f"전송일: {now.strftime('%Y-%m-%d %a %H:%M KST')}\n"
        f"(주말 미전송, 월요일은 금↔목 비교)\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    lines, MAX_LINES = [], 80
    for i, row in enumerate(result.itertuples(index=False), start=1):
        if i > MAX_LINES:
            lines.append(f"... (외 {len(result) - MAX_LINES}종 더 있음)")
            break
        v1 = safe_int(row.거래량_전일)
        v0 = safe_int(row.거래량_전전일)
        lines.append(
            f"{i}. {row.티커} {row.종목명 or ''} ({row.시장}) "
            f"{row.배수}x  {v1:,} vs {v0:,}"
        )
    return header + "\n" + "\n".join(lines)

if __name__ == "__main__":
    try:
        msg = build_report()
        if msg is not None:  # 주말이면 전송 안 함
            tg_send(msg)
        else:
            # 조용히 종료(로그만 남기고 싶으면 아래 한 줄 주석 해제)
            # tg_send("주말이므로 보고 스킵합니다.")
            pass
    except Exception as e:
        try:
            tg_send(f"⚠️ 자동화 에러: {e}")
        except Exception:
            pass
        sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)

