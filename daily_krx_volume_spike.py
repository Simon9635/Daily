#!/usr/bin/env python3
import os
import sys
import json
import time
import datetime as dt
from urllib import request, parse

# ---- 텔레그램 env ----
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ---- KRX 데이터 (pykrx) ----
#   pip install pykrx pandas
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
    """
    base_date 기준으로 과거로 내려가며
    '전일'과 '전전일'에 해당하는 실제 거래일 2개를 찾아서 yyyymmdd 문자열과 date를 함께 반환.
    """
    found = []
    cursor = base_date
    # 최대 15일 탐색 (연휴 대비)
    for _ in range(15):
        datestr = yyyymmdd(cursor)
        try:
            # KOSPI에서만 체크해도 '거래일 여부' 판정 가능
            df = stock.get_market_ohlcv_by_ticker(datestr, market="KOSPI")
            if df is not None and len(df) > 0:
                found.append(cursor)
                if len(found) == 2:
                    break
        except Exception:
            pass
        cursor -= dt.timedelta(days=1)
    if len(found) < 2:
        raise RuntimeError("거래일 2개를 찾지 못했습니다. (연휴/네트워크 이슈)")
    # found[0] = 전일, found[1] = 전전일
    d1, d0 = found[0], found[1]
    return yyyymmdd(d1), yyyymmdd(d0), d1, d0

def get_volume_by_market(datestr: str, market: str) -> pd.DataFrame:
    """
    특정 거래일(datestr)과 시장(market: KOSPI/KOSDAQ)에 대해
    티커별 거래량 DataFrame 반환 (티커, 거래량, 시장)
    """
    df = stock.get_market_ohlcv_by_ticker(datestr, market=market)
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["티커", "거래량", "시장"])
    df = df.reset_index().rename(columns={"티커": "티커"})  # 안전용
    # pykrx 컬럼은 보통 '거래량'으로 제공
    out = df[["티커", "거래량"]].copy()
    out["시장"] = market
    return out

def build_report():
    now_kst = dt.datetime.now(KST)
    # 오전 7시 실행 기준, 분석 기준일의 '전일'을 찾기 위해 어제 날짜를 베이스로 시작
    base = (now_kst - dt.timedelta(days=1)).date()
    d1_str, d0_str, d1_date, d0_date = find_two_trading_days(base)

    # 두 시장 데이터 한번에 수집
    frames_d1 = []
    frames_d0 = []
    for mkt in ["KOSPI", "KOSDAQ"]:
        frames_d1.append(get_volume_by_market(d1_str, mkt))
        frames_d0.append(get_volume_by_market(d0_str, mkt))
    vol1 = pd.concat(frames_d1, ignore_index=True)
    vol0 = pd.concat(frames_d0, ignore_index=True)

    # 병합(전일 vs 전전일)
    merged = pd.merge(vol1, vol0, on=["티커"], how="inner", suffixes=("_전일", "_전전일"))
    # 시장 정보는 전일 기준으로
    merged["시장"] = merged["시장_전일"] if "시장_전일" in merged.columns else merged.get("시장", "KRX")

    # 0 회피 및 배수 계산
    merged["거래량_전전일"].replace(0, pd.NA, inplace=True)
    merged = merged.dropna(subset=["거래량_전전일"])
    merged["배수"] = (merged["거래량_전일"] / merged["거래량_전전일"]).round(2)

    # 5배 이상 필터
    result = merged[merged["배수"] >= 5].copy()

    # 종목명 붙이기 (해당 거래일 기준 이름)
    tickers = result["티커"].tolist()
    name_map = {}
    # 이름 조회는 다건 반복이지만 성능상 수백개 내에서 충분
    for t in tickers:
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = ""
    result["종목명"] = result["티커"].map(name_map)

    # 보기 좋게 정렬 (배수 desc, 전일 거래량 desc)
    result.sort_values(by=["배수", "거래량_전일"], ascending=[False, False], inplace=True)

    # 텔레그램 메시지 구성 (길이 제한 4096자 주의 → 상위 N개만 표시)
    header = (
        f"<b>[KOSPI/KOSDAQ 거래량 급증 리스트]</b>\n"
        f"기준: 전일 {yyyy_mm_dd(d1_date)} vs 전전일 {yyyy_mm_dd(d0_date)}\n"
        f"조건: 전일 거래량 ≥ 전전일의 <b>5배</b>\n"
    )

    if len(result) == 0:
        return header + "\n해당 없음."

    lines = []
    MAX_LINES = 80  # 안전상 상위 80개까지만 출력
    for i, row in enumerate(result.itertuples(index=False), start=1):
        if i > MAX_LINES:
            lines.append(f"... (외 {len(result) - MAX_LINES}종 더 있음)")
            break
        lines.append(
            f"{i}. {row.티커} {row.종목명 or ''} ({row.시장})  "
            f"{row.배수}x  {int(row.거래량_전일):,} vs {int(row.거래량_전전일):,}"
        )

    msg = header + "\n" + "\n".join(lines)
    return msg

if __name__ == "__main__":
    try:
        msg = build_report()
        tg_send(msg)
    except Exception as e:
        # 에러도 텔레그램으로 알려주기 (운영 편의)
        try:
            tg_send(f"⚠️ 자동화 에러: {e}")
        except Exception:
            pass
        # 로그로도 남기고 종료
        sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)

