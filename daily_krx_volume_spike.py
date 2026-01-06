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

#!/usr/bin/env python3
# ... (위쪽 import 및 함수들은 기존과 동일하게 유지) ...

# [수정] 로그를 출력하도록 변경된 날짜 계산 함수
def pick_compare_days(now_kst: dt.datetime) -> tuple[dt.date, dt.date]:
    try:
        target_date = now_kst.date()
        print(f"🔍 [디버깅] 오늘 날짜 확인: {target_date}")
        
        end_str = target_date.strftime("%Y%m%d")
        start_str = (target_date - dt.timedelta(days=14)).strftime("%Y%m%d")
        
        # 데이터 조회 시도
        df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker="005930")
        
        if df.empty or len(df) < 2:
            print("❌ [디버깅] 삼성전자 데이터 조회 실패 (비어있음)")
            return None, None
            
        valid_dates = df.index.tolist()
        last_biz_day = valid_dates[-1].date()
        print(f"🔍 [디버깅] DB상 가장 최근 거래일: {last_biz_day}")
        
        if last_biz_day != target_date:
            print(f"⛔ [디버깅] 오늘은 개장일이 아닙니다. (오늘: {target_date} != 최근거래일: {last_biz_day})")
            # [중요] 장 마감 전(오후 3:30 이전)에 돌리면 데이터가 없어서 이쪽으로 빠질 수 있음
            return None, None
            
        d1 = valid_dates[-1].date()
        d0 = valid_dates[-2].date()
        print(f"✅ [디버깅] 날짜 확정: 오늘({d1}) vs 어제({d0})")
        
        return d1, d0
        
    except Exception as e:
        print(f"❌ [디버깅] 날짜 계산 중 에러 발생: {e}")
        return None, None

# ... (중간 함수들 동일) ...

# [수정] 메인 실행부 (로그 출력 추가)
if __name__ == "__main__":
    print("🚀 스크립트 시작")
    
    # 1. 토큰 확인
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ [치명적 에러] Github Secrets(토큰)가 설정되지 않았습니다.")
        sys.exit(1)
    else:
        print("✅ 토큰 감지됨 (보안상 숨김)")

    try:
        # 2. 리포트 생성 시도
        print("📊 리포트 생성 중...")
        msg = build_report()
        
        if msg is None:
            print("⚠️ [결과] 생성된 메시지가 없습니다. (휴장일이거나 데이터 부족)")
        else:
            print("📨 [전송] 텔레그램 메시지 전송 시도...")
            tg_send(msg)
            print("✅ [완료] 전송 로직 수행됨")
            
    except Exception as e:
        # 3. 에러 발생 시 봇으로 알림
        err_msg = f"⚠️ 자동화 에러 발생: {e}"
        print(err_msg)
        try:
            tg_send(err_msg)
        except:
            pass
        sys.exit(1)

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
