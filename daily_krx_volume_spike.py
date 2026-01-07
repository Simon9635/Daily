#!/usr/bin/env python3
import os
import sys
import time
import json
import html
import datetime as dt
from urllib import request, parse
import unicodedata

# --- [필수 라이브러리] ---
import requests
import FinanceDataReader as fdr  # 차트 분석용
from pykrx import stock          # 데이터 수집용
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

# ---------- 날짜 계산 ----------
def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    """
    DB(삼성전자)에 존재하는 '가장 최근 거래일(d1)'과 '그 직전 거래일(d0)'을 반환.
    오늘 데이터가 아직 집계 전이거나 장 중이면, 안전하게 하루 전 데이터를 사용.
    """
    try:
        target_date = now_kst.date()
        print(f"🔎 [날짜분석] 오늘: {target_date}")
        
        # 넉넉하게 2주치 조회
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        # 삼성전자(005930)로 거래일 확인
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        if df.empty or len(df) < 2:
            print("❌ [날짜분석] 거래일 데이터를 불러올 수 없습니다.")
            return None, None
            
        valid_dates = df.index.tolist()
        last_db_date = valid_dates[-1].date()
        
        # DB 마지막 날짜가 '오늘'이면 -> 아직 장 마감 데이터가 불안정할 수 있으므로 '어제' vs '그제'로 변경
        if last_db_date == target_date:
            print("💡 [날짜조정] 오늘 데이터 감지됨 → '어제' 마감 기준으로 변경합니다.")
            d1 = valid_dates[-2].date()
            d0 = valid_dates[-3].date()
        else:
            # DB 마지막 날짜가 어제(또는 그 이전)임 -> 확정된 데이터
            d1 = valid_dates[-1].date()
            d0 = valid_dates[-2].date()
            
        print(f"✅ [최종선정] 기준일: {d1} vs 대조일: {d0}")
        return d1, d0
    except Exception as e:
        print(f"❌ [날짜분석 에러] {e}")
        return None, None

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def yyyy_mm_dd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# ---------- [핵심 수정] 데이터 수집 통합 함수 ----------
def get_market_data(datestr: str, market: str) -> pd.DataFrame:
    """
    get_market_cap_by_ticker 기반
    거래대금 / 시가총액을 안정적으로 수집
    """
    max_retries = 3

    for i in range(max_retries):
        try:
            time.sleep(1)  # 연속 호출 방지

            df = stock.get_market_cap_by_ticker(datestr, market=market)

            if df is None or df.empty:
                raise ValueError("빈 데이터")

            df = df.reset_index()

            cols = df.columns.tolist()

            # ---- 컬럼 후보 정의 ----
            val_candidates = ["거래대금", "거래대금(원)", "거래금액"]
            cap_candidates = ["시가총액", "시가총액(원)", "시총"]

            val_col = next((c for c in val_candidates if c in cols), None)
            cap_col = next((c for c in cap_candidates if c in cols), None)

            if not val_col or not cap_col:
                raise ValueError(
                    f"필수 컬럼 누락 | columns={cols}"
                )

            out = df[["티커", val_col, cap_col]].copy()
            out.rename(
                columns={
                    val_col: "거래대금",
                    cap_col: "시가총액",
                },
                inplace=True
            )

            # 숫자형 변환 (안전)
            out["거래대금"] = (
                pd.to_numeric(out["거래대금"], errors="coerce")
                .fillna(0)
                .astype("int64")
            )
            out["시가총액"] = (
                pd.to_numeric(out["시가총액"], errors="coerce")
                .fillna(0)
                .astype("int64")
            )

            return out

        except Exception as e:
            print(f"⚠️ [재시도 {i+1}/{max_retries}] {market} {datestr} 수집 실패: {e}")
            time.sleep(2)

    print(f"❌ 최종 실패: {market} {datestr}")
    return pd.DataFrame(columns=["티커", "거래대금", "시가총액"])


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
    import unicodedata

    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width: return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    now = dt.datetime.now(KST)
    d1_date, d0_date = pick_compare_days(now)
    
    if d1_date is None:
        return None

    d1_str = yyyymmdd(d1_date)
    d0_str = yyyymmdd(d0_date)

    print("⏳ 데이터 수집 시작...")
    dfs_1, dfs_0 = [], []
    
    for mkt in ["KOSPI", "KOSDAQ"]:
        dfs_1.append(get_market_data(d1_str, mkt))
        dfs_0.append(get_market_data(d0_str, mkt))

    # 데이터 합치기
    df_1 = pd.concat(dfs_1, ignore_index=True)
    df_0 = pd.concat(dfs_0, ignore_index=True)

    if df_1.empty or df_0.empty:
        print("❌ [중단] 데이터프레임이 비어 있어 리포트를 생성할 수 없습니다.")
        return None

    # [데이터 병합] T-1일 데이터와 T-2일 데이터 매칭
    # 필요한 컬럼만 남겨서 병합 (시가총액은 최신 날짜인 df_1 것만 사용)
    df_1_sub = df_1[["티커", "거래대금", "시가총액"]]
    df_0_sub = df_0[["티커", "거래대금"]]
    
    merged = pd.merge(df_1_sub, df_0_sub, on="티커", how="inner", suffixes=("_1", "_0"))
    
    # [필터링]
    # 1. 전전일 거래대금이 0이 아니어야 함 (나눗셈 에러 방지)
    # 2. 거래대금 5배(500%) 이상 급증
    merged = merged[merged["거래대금_0"] > 0]
    merged["배수"] = merged["거래대금_1"] / merged["거래대금_0"]
    
    target = merged[merged["배수"] >= 5.0].copy()
    print(f"📊 1차 필터(5배 이상): {len(target)}개 종목 발견")

    # 정렬: 거래대금(당일) 내림차순 -> 상위 30개
    target = target.sort_values(by="거래대금_1", ascending=False).head(30)

    # 종목명 가져오기
    names = []
    tickers = target["티커"].tolist()
    for t in tickers:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append(t)

    # 금액 포맷팅 (억원)
    amts = []
    for val in target["거래대금_1"].tolist():
        amts.append(f"{val/100000000:,.1f}")

    # ===== 메시지 작성 =====
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 \n"
        f"sk 김수민입니다\n"
        f"전일거래대금 급증 종목 공유드립니다!\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    W_NUM = 3; W_NAME = 12; W_AMT = 12; W_CHART = 10; GAP = "   "

    h_line = (
        f"{' '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align('거래대금(억)', W_AMT)}{GAP}"
        f"{center_align('차트', W_CHART)}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 48)

    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME)
        amt_s = center_align(av, W_AMT)
        stat = get_chart_status(t_code)
        chart_s = center_align(stat, W_CHART)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}{GAP}{chart_s}"
        lines.append(f"<code>{html.escape(row)}</code>")

    return header + "\n".join(lines)

# ---------- 실행부 ----------
if __name__ == "__main__":
    print("🚀 스크립트 실행 시작")
    if not BOT_TOKEN:
        print("⚠️ [주의] 봇 토큰이 설정되지 않았습니다.")

    try:
        msg = build_report()
        if msg:
            print("📨 메시지 전송 시도...")
            tg_send(msg)
            print("✅ 전송 완료")
        else:
            print("⚠️ 생성된 메시지가 없습니다.")
    except Exception as e:
        print(f"❌ [치명적 에러] {e}")
        # 필요 시 에러도 텔레그램으로 전송
        # tg_send(f"⚠️ 에러 발생: {e}")
        sys.exit(1)
