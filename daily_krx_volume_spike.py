#!/usr/bin/env python3
import os
import sys
import json
import datetime as dt
from urllib import request, parse

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

def find_two_trading_days(base_date: dt.date) -> tuple[str, str, dt.date, dt.date]:
    found = []
    cursor = base_date
    for _ in range(15):  # 연휴 대비 최대 15일 역탐색
        datestr = yyyymmdd(cursor)
        try:
            df = stock.get_market_ohlcv_by_ticker(datestr, market="KOSPI")
            if df is not None and len(df) > 0:
                found.append(cursor)
                if len(found) == 2:
                    break
        except Exception:
            pass
        cursor -= dt.timedelta(days=1)
    if len(found) < 2:
        raise RuntimeError("거래일 2개를 찾지 못했습니다.")
    d1, d0 = found[0], found[1]  # d1=전일, d0=전전일
    return yyyymmdd(d1), yyyymmdd(d0), d1, d0

def get_volume_by_market(datestr: str, market: str) -> pd.DataFrame:
    """지정 시장의 해당일 티커/거래량/시장 반환. 거래량은 숫자형으로 강제."""
    df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["티커", "거래량", "시장"])
    df = df.reset_index()  # index=티커 -> 컬럼화
    # 컬럼 방어적으로 접근
    if "거래량" not in df.columns:
        # pykrx 버전별 컬럼명이 달라질 가능성 방지
        possible = [c for c in df.columns if "량" in c]
        if not possible:
            return pd.DataFrame(columns=["티커", "거래량", "시장"])
        vol_col = possible[0]
    else:
        vol_col = "거래량"
    out = df[["티커", vol_col]].copy()
    out.rename(columns={vol_col: "거래량"}, inplace=True)

    # 📌 여기서 숫자형 강제 변환(쉼표/문자 → NaN → 후처리)
    out["거래량"] = pd.to_numeric(out["거래량"], errors="coerce")
    out.dropna(subset=["거래량"], inplace=True)
    out["거래량"] = out["거래량"].astype("int64")  # 정수화(원하면 int64 유지)

    out["시장"] = market
    return out

def safe_int(n):
    try:
        return int(n)
    except Exception:
        return 0

def build_report():
    now_kst = dt.datetime.now(KST)
    base = (now_kst - dt.timedelta(days=1)).date()  # 어제 기준으로 전일/전전일 탐색
    d1_str, d0_str, d1_date, d0_date = find_two_trading_days(base)

    frames_d1, frames_d0 = [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        frames_d1.append(get_volume_by_market(d1_str, mkt))
        frames_d0.append(get_volume_by_market(d0_str, mkt))
    vol1 = pd.concat(frames_d1, ignore_index=True) if frames_d1 else pd.DataFrame(columns=["티커","거래량","시장"])
    vol0 = pd.concat(frames_d0, ignore_index=True) if frames_d0 else pd.DataFrame(columns=["티커","거래량","시장"])

    merged = pd.merge(vol1, vol0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))

    # 시장 컬럼 정리(전일 기준 시장 사용)
    if "시장_전일" in merged.columns:
        merged["시장"] = merged["시장_전일"]
    elif "시장" in merged.columns:
        pass
    else:
        merged["시장"] = "KRX"

    # 📌 숫자형 강제 변환(병합 후에도 한 번 더 보정)
    for col in ["거래량_전일", "거래량_전전일"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    # 0/NaN 제거
    merged = merged.dropna(subset=["거래량_전일", "거래량_전전일"]).copy()
    merged = merged[merged["거래량_전전일"] > 0]

    # 배수 계산
    merged["배수"] = (merged["거래량_전일"] / merged["거래량_전전일"]).round(2)

    # 5배 이상 필터
    result = merged[merged["배수"] >= 5].copy()

    # 종목명 매핑
    tickers = result["티커"].tolist()
    name_map = {}
    for t in tickers:
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # 정렬
    result.sort_values(by=["배수", "거래량_전일"], ascending=[False, False], inplace=True)

    header = (
        f"<b>[KOSPI/KOSDAQ 거래량 급증 리스트]</b>\n"
        f"기준: 전일 {yyyy_mm_dd(d1_date)} vs 전전일 {yyyy_mm_dd(d0_date)}\n"
        f"조건: 전일 거래량 ≥ 전전일의 <b>5배</b>\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    lines = []
    MAX_LINES = 80
    for i, row in enumerate(result.itertuples(index=False), start=1):
        if i > MAX_LINES:
            lines.append(f"... (외 {len(result) - MAX_LINES}종 더 있음)")
            break
        vol1 = safe_int(row.거래량_전일)
        vol0 = safe_int(row.거래량_전전일)
        lines.append(
            f"{i}. {row.티커} {row.종목명 or ''} ({row.시장})  "
            f"{row.배수}x  {vol1:,} vs {vol0:,}"
        )

    return header + "\n" + "\n".join(lines)

if __name__ == "__main__":
    try:
        msg = build_report()
        tg_send(msg)
    except Exception as e:
        try:
            tg_send(f"⚠️ 자동화 에러: {e}")
        except Exception:
            pass
        sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)
